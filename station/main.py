import os
import time
import random
import requests
import uuid
from datetime import datetime, timezone

WORKER_URL = os.getenv("WORKER_URL", "http://worker:8001")
FRECUENCY_MS = int(os.getenv("FRECUENCY_MS", 500))
STATION_ID = f"SN-MAN-{uuid.uuid4().hex[:4].upper()}"

def generate_payload():
    return {
        "departamento": "Caldas",
        "pais": "Colombia",
        "ultima_actualizacion": datetime.now(timezone.utc).isoformat(),
        "sensores_climaticos": [{
            "id_sensor": STATION_ID,
            "municipio": "Manizales",
            "latitud": 5.0689 + random.uniform(-0.01, 0.01),
            "longitud": -75.5174 + random.uniform(-0.01, 0.01),
            "lecturas": {
                "temperatura": {"valor": round(random.uniform(12.0, 25.0), 2), "unidad": "°C"},
                "presion_atmosferica": {"valor": round(random.uniform(1010.0, 1015.0), 1), "unidad": "hPa"},
                "velocidad_viento": {"valor": round(random.uniform(0.0, 30.0), 1), "unidad": "km/h"},
                "radiacion_solar": {"valor": round(random.uniform(100.0, 800.0), 1), "unidad": "W/m2"}
            },
            "estado": "activo"
        }]
    }

if __name__ == "__main__":
    print(f"Estación {STATION_ID} iniciada. Enviando datos a {WORKER_URL} cada {FRECUENCY_MS}ms...")
    
    time.sleep(10) # Pequeño delay inicial para que los workers arranquen
    
    while True:
        payload = generate_payload()
        try:
            requests.post(f"{WORKER_URL}/ingest", json=payload, timeout=2)
        except Exception as e:
            pass 
        time.sleep(FRECUENCY_MS / 1000.0)