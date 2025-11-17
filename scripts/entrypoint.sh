#!/bin/sh

echo "⏳ Esperando a que Kafka esté disponible..."
while ! nc -z kafka 9092; do
  sleep 2
done
echo "✅ Kafka listo."

echo "⏳ Esperando a que PostgreSQL esté disponible..."
while ! nc -z postgres-dwh 5432; do
  sleep 2
done
echo "✅ PostgreSQL listo."

echo "🚀 Iniciando consumidor Kafka → PostgreSQL..."
python kafka_consumer_postgres.py
