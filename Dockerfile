FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias básicas del sistema si fueran necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt-get/lists/*

# Copiar e instalar requerimientos de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del proyecto
COPY . .

# Comando por defecto (asume que crearemos un script simulador de test)
CMD ["python", "demo_simulation.py"]
