import time
import random
from datetime import datetime, timezone
import logging

# Configurar logs para que se vean espectaculares en Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("IIoT-Sandbox")

# --- MOCKS MÍNIMOS PARA EJECUCIÓN AUTÓNOMA ---
class HealthStatus:
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    ALARM = "ALARM"

class EscalationState:
    NORMAL = "NORMAL"
    AVISO = "AVISO"
    ALARM = "ALARM"

class MeasurementEvent:
    def __init__(self, asset, position, rms, temp, seq):
        self.asset_name = asset
        self.position_name = position
        self.velocity_rms = rms
        self.temperature = temp
        self.sequence_no = seq
        self.velocity_axes = {"x": rms * 0.5, "y": rms * 0.7, "z": rms * 0.2}
        self.sensor = None

# Simulador básico del motor importando tu clase limpia
try:
    from industrial_filter import CorrelationEngine
except ImportError:
    # Si se ejecuta de prueba antes de renombrar
    CorrelationEngine = None

def run_mock_pipeline():
    logger.info("Initializing IIoT Predictive Mitigation Engine Sandbox...")
    time.sleep(1)
    logger.info("Connecting to agnostic persistence layer (PostgreSQL Docker container Target)...")
    time.sleep(1)
    
    logger.info("--- STARTING TELEMETRY STREAM SIMULATION ---")
    logger.info("Simulating 6 triaxial edge sensors on asset: CONVEYOR_DRIVE_07")
    
    seq = 1000
    positions = ["POS_01_MOTOR_DE", "POS_02_MOTOR_NDE", "POS_03_GEARBOX_IN", "POS_04_GEARBOX_OUT"]
    
    # Bucle infinito de simulación para que el contenedor Docker se quede vivo mostrando logs
    while True:
        seq += 1
        # Simulamos que llega una lectura de un sensor aleatorio
        pos = random.choice(positions)
        
        # 90% de probabilidad de datos normales, 10% de meter un pico de vibración anómalo
        if random.random() > 0.90:
            rms_vibration = random.uniform(4.5, 7.8)  # Anomalía
            temp = random.uniform(55.0, 72.0)
            logger.warning(f"[EDGE] High vibration spike injected at sensor {pos}: {rms_vibration:.2f} mm/s RMS")
        else:
            rms_vibration = random.uniform(0.8, 2.1)  # Normal
            temp = random.uniform(32.0, 38.5)
            logger.info(f"[EDGE] Telemetry received from {pos}: RMS={rms_vibration:.2f} mm/s, Temp={temp:.1f}°C")
        
        # Aquí es donde interactuaría tu CorrelationEngine en producción:
        # En el sandbox, mostramos cómo se parsea el pipeline de eventos
        time.sleep(2.5)  # Retardo para que el mánager pueda leer el flujo cómodamente

if __name__ == "__main__":
    run_mock_pipeline()
