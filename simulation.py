import time
import random
from datetime import datetime, timezone
import logging

# =====================================================================
# CONFIGURACIÓN DE LOGS DE ALTA VISIBILIDAD PARA ENTORNO DOCKER
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("IIoT-Sandbox")

# =====================================================================
# INTERFACES Y ESTRUCTURAS DE DATOS DE SIMULACIÓN (MOCKS)
# =====================================================================
class HealthStatus:
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    ALARM = "ALARM"

class EscalationState:
    NORMAL = "NORMAL"
    AVISO = "AVISO"
    ALARM = "ALARM"

class MockSensor:
    def __init__(self, physical_id: str):
        self.physical_id = physical_id

class MeasurementEvent:
    """Mimetiza el objeto esperado por el pipeline del motor principal."""
    def __init__(self, asset_name: str, position_name: str, velocity_rms: float, temperature: float, sequence_no: int):
        self.asset_name = asset_name
        self.position_name = position_name
        self.velocity_rms = velocity_rms
        self.temperature = temperature
        self.sequence_no = sequence_no
        self.velocity_axes = {
            "x": round(velocity_rms * random.uniform(0.4, 0.6), 4),
            "y": round(velocity_rms * random.uniform(0.6, 0.8), 4),
            "z": round(velocity_rms * random.uniform(0.1, 0.3), 4)
        }
        self.sensor = MockSensor(physical_id=f"SN-{random.randint(10000, 99999)}")

# =====================================================================
# GATEWAY DE IMPORTACIÓN DE LA ARQUITECTURA REAL
# =====================================================================
ENGINE_AVAILABLE = False
try:
    from industrial_filter import CorrelationEngine
    ENGINE_AVAILABLE = True
    logger.info("Production Industrial-Filter module detected successfully.")
except ImportError:
    CorrelationEngine = None
    logger.warning("Running in Standalone Showcase Mode (High-Fidelity Architectural Emulation active).")

# =====================================================================
# PIPELINE DE EJECUCIÓN DEL SIMULADOR
# =====================================================================
def run_mock_pipeline():
    logger.info("Initializing IIoT Predictive Mitigation Engine Sandbox...")
    time.sleep(1)
    
    # Inicialización condicional del motor real o del entorno agnóstico
    engine = None
    if ENGINE_AVAILABLE and CorrelationEngine:
        try:
            engine = CorrelationEngine()
            logger.info("Core Analytics Engine (CorrelationEngine) successfully instantiated.")
        except Exception as e:
            logger.error(f"Failed to initialize production engine due to missing ecosystem dependencies: {e}")
            logger.info("Switching back to High-Fidelity Architectural Emulation.")
            engine = None

    logger.info("Connecting to agnostic persistence layer target (PostgreSQL Endpoint Ready)...")
    time.sleep(1)
    
    logger.info("=====================================================================")
    logger.info("--- STARTING TELEMETRY STREAM SIMULATION (Press Ctrl+C to stop) ---")
    logger.info("=====================================================================")
    
    # Parámetros del gemelo digital simulado
    asset = "CONVEYOR_DRIVE_07"
    positions = ["POS_01_MOTOR_DE", "POS_02_MOTOR_NDE", "POS_03_GEARBOX_IN", "POS_04_GEARBOX_OUT"]
    seq = 42000
    
    # Buffer interno de simulación para emular la lógica del script principal si corre aislado
    local_buffer = {}

    while True:
        try:
            seq += 1
            pos = random.choice(positions)
            
            # Inyección probabilística de anomalías físicas (12% de probabilidad de pico de fallo)
            is_anomaly = random.random() > 0.88
            if is_anomaly:
                rms = random.uniform(5.2, 8.4)  # Supera los límites estadísticos estándar
                temp = random.uniform(58.0, 74.5)
                logger.warning(f"[EDGE-GATEWAY] Anomalous telemetry chunk captured at {pos} | RMS={rms:.2f} mm/s, Temp={temp:.1f}°C")
            else:
                rms = random.uniform(0.9, 2.3)  # Rangos estables operativos
                temp = random.uniform(31.5, 39.0)
                logger.info(f"[EDGE-GATEWAY] Ingested packet from {pos} | RMS={rms:.2f} mm/s, Temp={temp:.1f}°C")

            # Crear el evento estructurado con vectores triaxiales empacados
            event = MeasurementEvent(
                asset_name=asset,
                position_name=pos,
                velocity_rms=rms,
                temperature=temp,
                sequence_no=seq
            )

            # --- ESCENARIO A: EJECUCIÓN SOBRE EL MOTOR DE PRODUCCIÓN REAL ---
            if engine is not None:
                try:
                    result = engine.process_measurement(event)
                    if result:
                        logger.info(f"[ENGINE-OUTPUT] Matrix batch processed. State: {result.escalation_state}")
                        if result.is_structural:
                            logger.warning(f"[ENGINE-ANALYTICS] Global Structural Resonance Detected (
