import json
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Configuración del consumidor Kafka
consumer = KafkaConsumer(
    'air_quality',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='air_quality_group'
)

# Conexión a PostgreSQL
conn = psycopg2.connect(
    host='postgres-dwh',
    port=5432,
    dbname='air_quality_dwh',  # ✅ Este es el nombre correcto
    user='airquality',
    password='airquality123'
)
cursor = conn.cursor()

# Crear tabla si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS air_quality_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        sensor_id TEXT,
        temperature REAL,
        humidity REAL,
        co2 REAL,
        pm25 REAL
    );
""")
conn.commit()

print("✅ Consumidor Kafka iniciado. Esperando mensajes...")

# Bucle de consumo
try:
    for message in consumer:
        data = message.value
        print(f"📥 Recibido: {data}")

        cursor.execute("""
            INSERT INTO air_quality_data (timestamp, sensor_id, temperature, humidity, co2, pm25)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            data['timestamp'],
            data['sensor_id'],
            data['temperature'],
            data['humidity'],
            data['co2'],
            data['pm25']
        ))
        conn.commit()
        print("💾 Insertado en PostgreSQL")
except KeyboardInterrupt:
    print("🛑 Interrupción manual. Cerrando conexión...")
finally:
    cursor.close()
    conn.close()
