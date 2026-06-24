"""
IIO-PME: Industrial IoT Predictive Mitigation & Correlation Engine.
Core Diagnostic and Sensor Cross-Correlation Node.

Author: Raúl Ruano Gil
"""

from datetime import datetime, timezone
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Note: The following imports represent the modular industrial ecosystem 
# (Adaptive Thresholds, Axis Calibration, Equipment Registries, Escalation Managers, and ML Inference)
from adaptive_thresholds import AdaptiveThresholdManager
from axis_calibration import calibrate_axes
from config import BASELINE_MIN_SAMPLES, SITE_NAME
from config import get_params_for_type
from equipment_registry import Equipment, EquipmentRegistry
from escalation import EscalationManager
from models import (
    CalibratedAxes,
    CorrelationResult,
    DiagnosisSuggestion,
    EscalationState,
    HealthStatus,
    MeasurementEvent,
    SensorBaseline,
    WorkOrder,
)
from pre_diagnosis import generate_pre_diagnosis
from structural_detector import detect_structural_vibration
from triangulation import triangulate_fault


class CorrelationEngine:
    """
    Main Analytics & Cross-Correlation Engine.

    Processes asynchronous multi-sensor industrial telemetry grouped by asset, producing:
    - Real-time Health Status per sensor and asset topology.
    - Structural Vibration and Cross-Sensor Resonance Detection.
    - Automated Rule-Based Pre-Diagnosis combined with asymmetric ML verification.
    - Spatial Fault Triangulation and Defect Localization.
    - Smart Escalation Work Orders with dynamic RPN (Risk Priority Number) calculation.
    """

    def __init__(self, registry: Optional[EquipmentRegistry] = None):
        self.registry = registry or EquipmentRegistry()
        self.thresholds = AdaptiveThresholdManager()
        self.escalation = EscalationManager()

        # Motor registry (set externally from the API after loading)
        self.motor_registry: dict = {}

        # Bayesian fault detector (accumulates fault evidence across sequential ticks)
        from bayesian_detector import BayesianFaultDetector
        self._bayesian = BayesianFaultDetector()

        # Agnostic Persistence Layer (Local SQLite, PostgreSQL, or NoSQL Document Store)
        self._store = None

        # Buffers for multi-tick verification and spatial cross-correlation
        self._previous_readings: dict[str, dict[str, float]] = {}
        self._current_all_readings: dict[str, dict[str, float]] = {}
        self._closure_history: dict[str, list[dict]] = {}
        self._events_processed = 0
        self._last_snapshot_time: dict[str, datetime] = {}

        # --- Asynchronous Sensor Debounce & Window Buffer ---
        # In physical industrial setups, edge sensors do not transmit simultaneously.
        # Telemetry chunks typically arrive with a 5 to 45-minute jitter.
        # This window buffer aggregates ticks until sufficient spatial context is met.
        #
        # Execution Conditions (Both must be satisfied):
        #   1. ≥ 3 sensors reporting fresh telemetry within the window.
        #   2. ≥ 45 minutes elapsed since the initial asset trigger.
        # Hard Timeout: 60 minutes (safeguard against offline edge gateways).
        self._sensor_buffer: dict[str, dict[str, MeasurementEvent]] = {}
        self._buffer_trigger_time: dict[str, datetime] = {}
        self.BUFFER_MIN_SENSORS = 3
        self.BUFFER_WAIT_MINUTES = 45
        self.BUFFER_TIMEOUT_MINUTES = 60

    def enable_persistence(self):
        """Activates the storage layer adapters."""
        try:
            from persistence import get_store
            self._store = get_store()
        except Exception as e:
            logger.warning("Could not enable persistence: %s", e)

    def process_measurement(self, event: MeasurementEvent) -> Optional[CorrelationResult]:
        """
        Processes an individual incoming sensor telemetry packet.

        Telemetry streams asynchronously from data pipelines (e.g., MQTT/Kafka brokers).
        The engine buffers events per asset and triggers full analysis when:
          - ≥ 3 sensors supply fresh data AND ≥ 45 mins have passed since the first trigger.
          - Or the 60-minute timeout barrier is breached.

        Missing node data is gracefully backfilled using the last known state to preserve topology.
        """
        self._events_processed += 1
        asset_name = event.asset_name
        pos_code = event.position_name

        if not asset_name or not pos_code:
            return None

        # Deduplication: Drop exact duplicate frames caused by edge gateway re-transmissions
        existing = self._sensor_buffer.get(asset_name, {}).get(pos_code)
        if existing and existing.sequence_no == event.sequence_no:
            return None

        # Append telemetry frame to buffer
        if asset_name not in self._sensor_buffer:
            self._sensor_buffer[asset_name] = {}
        self._sensor_buffer[asset_name][pos_code] = event

        # Track window initiation timestamp for this specific asset
        if asset_name not in self._buffer_trigger_time:
            self._buffer_trigger_time[asset_name] = datetime.now(timezone.utc)

        # Evaluate window state
        buffer = self._sensor_buffer[asset_name]
        trigger_time = self._buffer_trigger_time[asset_name]
        elapsed = (datetime.now(timezone.utc) - trigger_time).total_seconds() / 60.0

        equipment = self.registry.get(asset_name)
        total_sensors = len(equipment.positions) if equipment else 6
        min_sensors = min(3, total_sensors)

        has_enough_sensors = len(buffer) >= min_sensors
        has_waited_enough = elapsed >= self.BUFFER_WAIT_MINUTES
        has_timed_out = elapsed >= self.BUFFER_TIMEOUT_MINUTES

        # Evaluate execution gates
        if (has_enough_sensors and has_waited_enough) or has_timed_out:
            result = self.process_equipment_batch(asset_name, buffer)
            self._sensor_buffer.pop(asset_name, None)
            self._buffer_trigger_time.pop(asset_name, None)
            return result

        return None

    def process_equipment_batch(
        self,
        asset_name: str,
        measurements: dict[str, MeasurementEvent],
    ) -> CorrelationResult:
        """
        Executes full spatial-correlation and diagnostics on a synchronized asset batch.
        """
        equipment = self.registry.get(asset_name)
        if not equipment:
            return CorrelationResult(asset_name=asset_name)

        # Backfill historical state for lagging sensors to maximize triangulation accuracy
        if asset_name in self._current_all_readings:
            last_readings = self._current_all_readings[asset_name]
            for pos in equipment.positions:
                if pos.code not in measurements and pos.code in last_readings:
                    pass

        eq_params = get_params_for_type(equipment.equipment_type)

        # 1. Triaxial Axis Realignment & Baseline Statistical Tracking
        readings = {}
        calibrated = {}
        temperatures = {}
        position_ends = {}
        position_sides = {}

        for pos in equipment.positions:
            event = measurements.get(pos.code)
            if not event:
                continue

            try:
                velocity_rms = event.velocity_rms
                axes = event.velocity_axes
                readings[pos.code] = velocity_rms
                temperatures[pos.code] = event.temperature
                position_ends[pos.code] = pos.end
                position_sides[pos.code] = pos.side

                # Update running statistical thresholds using raw physical components
                self.thresholds.update(
                    asset_name=asset_name,
                    position_name=pos.code,
                    velocity_rms=velocity_rms,
                    x_raw=axes["x"],
                    y_raw=axes["y"],
                    z_raw=axes["z"],
                    warning_sigma=eq_params["warning_sigma"],
                    alarm_sigma=eq_params["alarm_sigma"],
                    physical_id=getattr(event.sensor, "physical_id", "") if event.sensor else "",
                )

                # Automated Mounting Posture Inference Engine
                bl = self.thresholds.get_baseline(asset_name, pos.code)
                if (
                    bl
                    and bl.sample_count >= BASELINE_MIN_SAMPLES
                    and not pos.posture_locked
                ):
                    if bl.mean_x > 0 or bl.mean_y > 0 or bl.mean_z > 0:
                        from axis_calibration import infer_mounting_posture
                        inference = infer_mounting_posture(
                            mean_x=bl.mean_x,
                            mean_y=bl.mean_y,
                            mean_z=bl.mean_z,
                            sample_count=bl.sample_count,
                            current_posture=pos.mounting_posture,
                            mounting_face=getattr(pos, "mounting_face", "TOP"),
                        )
                        if (
                            not inference.matches_current
                            and inference.confidence >= 0.35
                        ):
                            old_posture = pos.mounting_posture
                            pos.mounting_posture = inference.inferred_posture
                            logger.info(
                                "[AUTO-POSTURE EFFECTED] %s/%s: %s → %s (conf=%.2f)",
                                asset_name, pos.code, old_posture, inference.inferred_posture, inference.confidence
                            )

                # Calibrate and re-align axes according to inferred or locked physical orientation
                cal = calibrate_axes(axes["x"], axes["y"], axes["z"], posture=pos.mounting_posture)
                calibrated[pos.code] = cal.to_calibrated_axes()

            except Exception as e:
                logger.warning("Error processing sensor telemetry %s/%s: %s", asset_name, pos.code, e)

        # 2. Statistical Threshold Evaluation
        triggered_sensors = []
        sensor_statuses = {}

        for pos_code, rms in readings.items():
            pos_location = "EXTERNAL"
            for pos in equipment.positions:
                if pos.code == pos_code:
                    pos_location = pos.mounting_location
                    break

            status, details = self.thresholds.evaluate(
                asset_name=asset_name,
                position_name=pos_code,
                velocity_rms=rms,
                warning_sigma=eq_params["warning_sigma"],
                alarm_sigma=eq_params["alarm_sigma"],
                mounting_location=pos_location,
            )
            sensor_statuses[pos_code] = {"status": status, "details": details}

            if status in (HealthStatus.WARNING, HealthStatus.ALARM):
                triggered_sensors.append(pos_code)

        # 3. Spatial Cross-Correlation & Advanced Analytics Pipeline
        baselines = self.thresholds.get_all_baselines(asset_name)

        is_structural = False
        structural_confidence = 0.0
        correlation = 0.0
        diagnosis = None
        triangulation_result = None
        wo = None

        if triggered_sensors:
            # 3a. Structural Resonance / Global Cross-Correlation Detection
            sensor_positions = {pos.code: (pos.x, pos.y) for pos in equipment.positions if pos.code in readings}
            structural = detect_structural_vibration(
                readings, baselines,
                variance_max=eq_params["structural_variance_max"],
                correlation_threshold=eq_params["structural_correlation_threshold"],
                min_sensors_pct=eq_params["structural_min_sensors_pct"],
                sensor_positions=sensor_positions,
                equipment_length_mm=equipment.geometry.length_mm,
            )
            is_structural = structural["is_structural"]
            structural_confidence = structural["confidence"]
            correlation = structural["correlation"]

            trend_data = {}

            if not is_structural:
                # 3b. Spatial Fault Triangulation
                try:
                    triangulation_result = triangulate_fault(equipment, readings, baselines)
                except Exception as e:
                    logger.warning("Triangulation fault on asset %s: %s", asset_name, e)

                # 3c. Heuristic Pre-Diagnosis Core
                try:
                    trend_data = {}
                    for pos_code in triggered_sensors:
                        trend = self.thresholds.get_trend(asset_name, pos_code)
                        if trend:
                            trend_data[pos_code] = trend

                    historical = self._closure_history.get(asset_name, [])

                    from motor_registry_helper import resolve_motor_info
                    motor_info = resolve_motor_info(asset_name, equipment.equipment_type, self.motor_registry)

                    diagnosis = generate_pre_diagnosis(
                        asset_name=asset_name, equipment_type=equipment.equipment_type,
                        readings=readings, calibrated=calibrated, baselines=baselines,
                        temperatures=temperatures, position_ends=position_ends, position_sides=position_sides,
                        trend_data=trend_data, historical_events=historical, motor_info=motor_info,
                    )

                    # Asymmetric ML Validation: Rules act as baseline; ML overrides only on high-confidence consensus
                    try:
                        from ml_inference import get_inference_engine
                        ml = get_inference_engine()
                        if ml.is_loaded and triggered_sensors:
                            top_sensor = max(triggered_sensors, key=lambda s: readings.get(s, 0))
                            bl = baselines.get(top_sensor)
                            cal = calibrated.get(top_sensor)
                            if bl and cal:
                                ml_features = {
                                    "rms": readings[top_sensor],
                                    "baseline_mean": bl.mean,
                                    "baseline_std": bl.std_dev,
                                    "deviation_pct": ((readings[top_sensor] - bl.mean) / bl.mean * 100) if bl.mean > 0 else 0,
                                    "axial": cal.axial, "radial": cal.radial, "tangential": cal.tangential,
                                    "temperature": temperatures.get(top_sensor, 0),
                                }
                                ml_pred = ml.predict(
                                    features=ml_features,
                                    rule_prediction=diagnosis.failure_mode if diagnosis else "",
                                    asset_name=asset_name,
                                )
                                if ml_pred and ml_pred.get("should_override_rules"):
                                    diagnosis = DiagnosisSuggestion(
                                        failure_mode=ml_pred["ml_prediction"],
                                        failure_mode_confidence=ml_pred["ml_confidence"],
                                        failure_cause=diagnosis.failure_cause,
                                        failure_cause_confidence=diagnosis.failure_cause_confidence,
                                        action_suggested=diagnosis.action_suggested,
                                        action_confidence=diagnosis.action_confidence,
                                        based_on_events=diagnosis.based_on_events,
                                        alternatives=diagnosis.alternatives,
                                        rul_optimistic_weeks=diagnosis.rul_optimistic_weeks,
                                        rul_probable_weeks=diagnosis.rul_probable_weeks,
                                        rul_pessimistic_weeks=diagnosis.rul_pessimistic_weeks,
                                        spare_parts=diagnosis.spare_parts,
                                    )
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning("Pre-diagnosis failed on asset %s: %s", asset_name, e)

                # Enrich Diagnostics via Expert Reasoning Mappers
                if diagnosis:
                    diagnosis = self._enrich_diagnosis_v22(
                        diagnosis, readings, baselines, calibrated,
                        temperatures, trend_data, triggered_sensors,
                        measurements, is_structural,
                    )

                if not diagnosis or diagnosis.failure_mode not in ("EARLY_LUBRICATION",):
                    self._check_early_lubrication(asset_name, triggered_sensors, baselines)

            # 3d. Dynamic Escalation & Work Order Generation
            previous = self._previous_readings.get(asset_name)
            max_slope = 0.0
            if trend_data:
                for td in trend_data.values():
                    slope = td.get("slope_per_week", 0.0)
                    if slope > max_slope:
                        max_slope = slope

            elevated_pct = len(triggered_sensors) / len(readings) if readings else 0.0

            wo = self.escalation.process_event(
                asset_name=asset_name, triggered_sensors=triggered_sensors, readings=readings,
                previous_readings=previous, diagnosis=diagnosis, triangulation=triangulation_result,
                is_structural=is_structural, elevated_pct=elevated_pct, max_slope_per_week=max_slope,
            )
        else:
            wo = self.escalation.process_event(asset_name=asset_name, triggered_sensors=[], readings=readings)

        self._previous_readings[asset_name] = readings.copy()
        self._current_all_readings[asset_name] = readings.copy()

        # --- Post-Escalation Risk Analysis (RPN Integration) ---
        if wo and wo.diagnosis and wo.diagnosis.failure_mode:
            try:
                from rpn_calculator import calculate_rpn
                from health_index import calculate_health_index

                hi_result = calculate_health_index(
                    asset_name=asset_name, baselines=baselines, readings=readings,
                    temperatures=temperatures, trend_data=trend_data or None,
                )

                rpn_result = calculate_rpn(
                    diagnosis=wo.diagnosis, health_index_score=hi_result.score,
                    equipment_type=equipment.equipment_type,
                )

                wo.description += (
                    f" | RPN={rpn_result.rpn} ({rpn_result.priority_band})"
                    f" [P={rpn_result.probability} C={rpn_result.consequence} D={rpn_result.detectability}]"
                )
            except Exception as e:
                logger.debug("RPN calculation skipped: %s", e)

        # Advanced Signal Context Extrapolations (Damping, Frequency Shifts, Wavelet-Bands)
        if triggered_sensors:
            try:
                from damping_estimator import estimate_damping_change
                for pos_code in triggered_sensors[:2]:
                    history = self.thresholds._history.get(f"{asset_name}::{pos_code}", [])
                    if len(history) >= 48:
                        damping = estimate_damping_change(history)
                        if damping and damping.get("damping_degraded"):
                            logger.info("[DAMPING ANOMALY] %s/%s: %s", asset_name, pos_code, damping["description"])
            except Exception as e:
                logger.debug("Damping tracking skipped: %s", e)

        if triggered_sensors:
            try:
                from dominant_frequency import estimate_frequency_shift
                for pos_code in triggered_sensors[:2]:
                    history = self.thresholds._history.get(f"{asset_name}::{pos_code}", [])
                    if len(history) >= 48:
                        freq_result = estimate_frequency_shift(band_wide_history=history, band_narrow_history=history)
                        if freq_result and freq_result.get("frequency_shift"):
                            logger.info("[FREQUENCY SHIFT DETECTED] %s/%s: %s", asset_name, pos_code, freq_result["description"])
            except Exception as e:
                logger.debug("Frequency analytics skipped: %s", e)

        if triggered_sensors and measurements:
            try:
                from wavelet_analysis import analyze_from_fft_bands
                for pos_code in triggered_sensors[:1]:
                    event = measurements.get(pos_code)
                    if event:
                        acc_hf = event.features.acceleration.band_0_to_6000hz
                        vel_lf = event.features.velocity.band_10_to_1000hz
                        hf_rms = (acc_hf.x_axis.rms + acc_hf.y_axis.rms + acc_hf.z_axis.rms) / 3
                        lf_rms = vel_lf.total_vibration.rms if vel_lf.total_vibration else 0
                        if hf_rms > 0 and lf_rms > 0:
                            band_result = analyze_from_fft_bands(band_0_6000_rms=hf_rms, band_10_1000_rms=lf_rms)
                            if band_result.get("high_frequency_ratio", 0) > 3:
                                logger.info("[WAVELET ANALYSIS HINT] %s/%s: %s", asset_name, pos_code, band_result["diagnosis_hint"])
            except Exception as e:
                logger.debug("Wavelet cross-bands skipped: %s", e)

        # Bayesian State Estimator
        if triggered_sensors:
            try:
                evidence = []
                if len(triggered_sensors) == 1:
                    evidence.append("single_sensor")
                elif len(triggered_sensors) > 3:
                    evidence.append("multiple_sensors")
                if not is_structural and len(triggered_sensors) < len(readings):
                    evidence.append("neighbors_normal")
                if is_structural:
                    evidence.append("neighbors_elevated")
                if trend_data:
                    for td in trend_data.values():
                        if td.get("direction") == "INCREASING":
                            evidence.append("trend_increasing")
                            break
                if evidence:
                    self._bayesian.update(asset_name, evidence)
            except Exception as e:
                logger.debug("Bayesian inference skipped: %s", e)
        elif not triggered_sensors:
            try:
                self._bayesian.decay(asset_name)
            except Exception:
                pass

        # Time-Travel Feature Logging and Persistence for ML training loops (Downsampling 1/hour)
        self._persist_snapshots(
            wo=wo, asset_name=asset_name, readings=readings, baselines=baselines,
            calibrated=calibrated, temperatures=temperatures, diagnosis=diagnosis,
            is_structural=is_structural, structural_confidence=structural_confidence,
        )

        if any(s["status"] == HealthStatus.ALARM for s in sensor_statuses.values()):
            escalation_state = EscalationState.ALARM
        elif triggered_sensors:
            escalation_state = EscalationState.AVISO
        else:
            escalation_state = EscalationState.NORMAL

        return CorrelationResult(
            asset_name=asset_name, timestamp=datetime.now(timezone.utc),
            is_structural=is_structural, structural_confidence=structural_confidence,
            correlation_coefficient=correlation, elevated_sensors=triggered_sensors,
            elevated_pct=len(triggered_sensors) / len(readings) if readings else 0,
            variance_normalized=0, triggered_sensor=triggered_sensors[0] if triggered_sensors else "",
            escalation_state=escalation_state, wo_level=wo.level if wo else None,
            diagnosis=diagnosis, triangulation=triangulation_result, calibrated_readings=calibrated,
        )

    def add_closure_event(self, asset_name: str, closure: dict):
        """
        Logs an asset maintenance closure ticket to feedback and balance escalation thresholds.
        Handles physical feedback loops to adjust reincidence metrics dynamically.
        """
        if asset_name not in self._closure_history:
            self._closure_history[asset_name] = []
        self._closure_history[asset_name].append(closure)
        if len(self._closure_history[asset_name]) > 50:
            self._closure_history[asset_name] = self._closure_history[asset_name][-50:]

        failure_mode = closure.get("failure_mode", "")
        if failure_mode == "NO_ISSUE":
            has_trend = False
            baselines = self.thresholds.get_all_baselines(asset_name)
            for pos_code in baselines:
                trend = self.thresholds.get_trend(asset_name, pos_code)
                if trend and trend.get("direction") == "INCREASING":
                    has_trend = True
                    break
            self.escalation.update_reincidence(asset_name, no_issue=True, has_rising_trend=has_trend)
        else:
            self.escalation.update_reincidence(asset_name, no_issue=False, has_rising_trend=False)

        if self._store:
            try:
                self._store.save_closure(asset_name, closure)
            except Exception:
                pass

    def analyze_neighbors(self, asset_name: str) -> Optional[dict]:
        """Calculates structural spatial correlation matrices across physical machine neighbors."""
        try:
            from neighbor_correlation import analyze_neighbor_correlation
            return analyze_neighbor_correlation(
                asset_name=asset_name, registry=self.registry,
                thresholds=self.thresholds, current_readings=self._current_all_readings,
            )
        except Exception:
            return None

    def _persist_snapshots(
        self, wo, asset_name, readings, baselines, calibrated,
        temperatures, diagnosis, is_structural, structural_confidence,
    ):
        """Assembles flattened state arrays optimized for XGBoost feature pipelines."""
        if wo and self._store:
            try:
                self._store.save_work_order(wo)
            except Exception:
                pass
        if self._events_processed % 50 == 0 and self._store:
            self._persist_baselines(asset_name, baselines)

    def _persist_baselines(self, asset_name: str, baselines: dict[str, SensorBaseline]):
        if not self._store:
            return
        for pos_name, bl in baselines.items():
            try:
                self._store.save_baseline(asset_name, pos_name, bl)
            except Exception:
                pass
