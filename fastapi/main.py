from fastapi import FastAPI
from minio import Minio
from datetime import datetime
import pandas as pd
import os
import io

app = FastAPI()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET_NAME = "air-quality-data"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

@app.post("/test_minio")
def test_minio():
    data = {
        "timestamp": datetime.now().isoformat(),
        "temperature": 22.5,
        "humidity": 50.1,
        "co2": 780,
        "pm25": 11.2,
        "pm10": 19.8
    }

    df = pd.DataFrame([data])
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    filename = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    try:
        minio_client.put_object(
            BUCKET_NAME,
            filename,
            data=io.BytesIO(csv_bytes),
            length=len(csv_bytes)
        )
        return {"status": "ok", "filename": filename}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
