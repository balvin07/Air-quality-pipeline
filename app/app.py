from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
from minio import Minio
from datetime import datetime
import json
import pandas as pd
import os

# -------------------------------
# 🔧 Configuración de servicios
# -------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "air-quality-data"

# -------------------------------
# 🚀 Inicialización
# -------------------------------
app = FastAPI(title="API IoT - Calidad del Aire")
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Crear bucket si no existe
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)


# -------------------------------
# 📡 Modelo de datos IoT
# -------------------------------
class AirQualityData(BaseModel):
    station_id: int
    temperature: float
    humidity: float
    co2: float
    pm25: float
    pm10: float
    timestamp: str = datetime.utcnow().isoformat()


# -------------------------------
# 📨 Endpoint para recibir datos IoT
# -------------------------------
@app.post("/ingest")
async def ingest_data(data: AirQualityData):
    try:
        # Convertir a JSON
        json_data = data.model_dump()
        serialized = json.dumps(json_data)

        # Enviar a Kafka
        producer.produce("air_quality", serialized.encode("utf-8"))
        producer.flush()

        # Guardar en MinIO como CSV (append incremental)
        file_name = "air_quality_data.csv"

        # Si ya existe el archivo en MinIO, descargarlo y anexar
        if minio_client.bucket_exists(BUCKET_NAME):
            try:
                response = minio_client.get_object(BUCKET_NAME, file_name)
                df_existing = pd.read_csv(response)
                df_new = pd.DataFrame([json_data])
                df_all = pd.concat([df_existing, df_new], ignore_index=True)
            except Exception:
                df_all = pd.DataFrame([json_data])
        else:
            df_all = pd.DataFrame([json_data])

        # Guardar CSV actualizado
        csv_bytes = df_all.to_csv(index=False).encode('utf-8')
        minio_client.put_object(
            BUCKET_NAME, file_name, data=csv_bytes, length=len(csv_bytes)
        )

        return {"status": "ok", "message": "Datos recibidos y enviados correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------
# 🧠 Endpoint de prueba
# -------------------------------
@app.get("/")
def root():
    return {"message": "🌍 API IoT de Calidad del Aire funcionando correctamente"}


# -------------------------------
# ▶️ Para desarrollo local
# -------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
