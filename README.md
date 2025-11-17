# 🌎 Sistema IoT para Monitoreo Ambiental

Este proyecto implementa una arquitectura distribuida para simular, procesar, almacenar y visualizar datos de calidad del aire en tiempo real. Utiliza contenedores Docker para orquestar servicios como Kafka, MinIO, PostgreSQL, FastAPI, Streamlit, Grafana y pgAdmin.

---

## 🧱 Arquitectura

![Diagrama de arquitectura](images/arquitectura.png)

- **Simulador**: genera datos de CO₂, PM2.5, temperatura y humedad
- **FastAPI**: expone endpoints REST y envía datos a Kafka
- **Kafka**: gestiona el flujo de datos en tiempo real
- **Kafka UI**: visualiza tópicos y mensajes
- **MinIO**: almacena archivos CSV simulados
- **PostgreSQL**: almacena datos estructurados
- **pgAdmin**: administra la base de datos
- **Streamlit**: visualiza datos en tiempo real
- **Grafana**: paneles con alertas y métricas

---

## 🚀 Cómo ejecutar

```bash
git clone https://github.com/balvin07/Air-quality-pipeline.git
cd Air-quality-pipeline
docker-compose up --build -d
