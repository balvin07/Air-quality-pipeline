import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import argparse

# Esperar a que Kafka esté disponible
def wait_for_kafka(bootstrap_servers, retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Conectado a Kafka.")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Esperando Kafka... intento {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("❌ No se pudo conectar a Kafka después de varios intentos.")

# Función para generar datos simulados
def generate_sensor_data():
    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id": f"sensor_{random.randint(1, 5)}",
        "temperature": round(random.uniform(18, 30), 2),
        "humidity": round(random.uniform(40, 70), 2),
        "co2": round(random.uniform(300, 800), 2),
        "pm25": round(random.uniform(5, 50), 2)
    }

# Bucle principal
def run_simulation(producer, interval):
    print(f"🚀 Simulador iniciado. Enviando datos cada {interval} segundos...")
    while True:
        data = generate_sensor_data()
        producer.send('air_quality', value=data)
        print(f"📤 Enviado: {data}")
        time.sleep(interval)

# Argumentos de línea de comandos
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador de sensores IoT para calidad del aire")
    parser.add_argument('--interval', type=int, default=2, help='Intervalo de envío en segundos')
    args = parser.parse_args()

    producer = wait_for_kafka('kafka:9092')
    run_simulation(producer, args.interval)
