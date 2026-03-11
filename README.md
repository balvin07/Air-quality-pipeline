# 🌎 Air Quality IoT Pipeline

Pipeline distribuido de extremo a extremo para ingesta, procesamiento, almacenamiento y visualización de datos de calidad del aire en tiempo real.

> Proyecto de portafolio que demuestra habilidades en Data Engineering: streaming con Kafka, almacenamiento con MinIO y PostgreSQL, APIs REST con FastAPI y dashboards con Streamlit y Grafana, todo orquestado con Docker Compose.

---

## 📐 Arquitectura

```
Simulador IoT
     │
     ▼
  Kafka (broker de mensajes)
     │
     ├──► FastAPI  ──► MinIO (almacenamiento CSV)
     │
     └──► Consumer ──► PostgreSQL (DWH)
                            │
                    ┌───────┴───────┐
                    ▼               ▼
                Streamlit        Grafana
              (dashboard)       (métricas)
```

| Componente     | Rol                                               | Puerto |
|----------------|---------------------------------------------------|--------|
| **Simulador**  | Genera datos sintéticos de sensores cada 2s       | —      |
| **FastAPI**    | API REST que recibe y enruta datos                | 8000   |
| **Kafka**      | Message broker para streaming en tiempo real      | 9092   |
| **Kafka UI**   | Visualiza tópicos y mensajes de Kafka             | 8080   |
| **MinIO**      | Object storage para archivos CSV                  | 9000 / 9001 |
| **PostgreSQL** | Data warehouse estructurado                       | 5432   |
| **pgAdmin**    | Admin UI para PostgreSQL                          | 5050   |
| **Streamlit**  | Dashboard interactivo de visualización            | 8501   |
| **Grafana**    | Paneles con alertas y métricas                    | 3000   |

---

# Inicio rápido

### Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/) >= 24
- [Docker Compose](https://docs.docker.com/compose/) >= 2.20
- 4 GB de RAM disponibles

### 1. Clonar el repositorio

```bash
git clone https://github.com/balvin07/Air-quality-pipeline.git
cd Air-quality-pipeline
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Edita .env si quieres cambiar credenciales
```

### 3. Levantar todos los servicios

```bash
docker-compose up --build -d
```

### 4. Verificar que todo esté corriendo

```bash
docker-compose ps
```

Espera ~30 segundos a que Kafka y PostgreSQL terminen su inicialización.

---

## 🖥️ Acceso a los servicios

| Servicio     | URL                          | Usuario   | Contraseña  |
|--------------|------------------------------|-----------|-------------|
| Streamlit    | http://localhost:8501        | —         | —           |
| Grafana      | http://localhost:3000        | admin     | admin123    |
| Kafka UI     | http://localhost:8080        | —         | —           |
| MinIO Console| http://localhost:9001        | admin     | admin123    |
| pgAdmin      | http://localhost:5050        | admin@airquality.com | admin123 |
| FastAPI Docs | http://localhost:8000/docs   | —         | —           |

---

## 📡 Datos simulados

El simulador genera lecturas cada 2 segundos con los siguientes campos:

| Campo         | Descripción                  | Rango típico |
|---------------|------------------------------|--------------|
| `sensor_id`   | ID del sensor (1–5)          | sensor_1 … sensor_5 |
| `temperature` | Temperatura en °C            | 18 – 30      |
| `humidity`    | Humedad relativa (%)         | 40 – 70      |
| `co2`         | Concentración CO₂ (ppm)      | 300 – 800    |
| `pm25`        | Material particulado PM2.5   | 5 – 50       |

---

## 🗂️ Estructura del proyecto

```
Air-quality-pipeline/
├── app/                        # Streamlit dashboard
│   ├── streamlit_app.py
│   ├── requirements.txt
│   └── Dockerfile.streamlit
├── fastapi/                    # API REST
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── scripts/                    # Simulador y consumidor Kafka
│   ├── simulator.py
│   ├── kafka_consumer_postgres.py
│   ├── entrypoint.sh
│   ├── requirements.txt
│   ├── Dockerfile.simulator
│   └── Dockerfile.consumer
├── grafana/
│   ├── provisioning/
│   └── dashboards/
├── data/                       # Volúmenes locales (ignorado en git)
├── docker-compose.yml
├── .env.example                # Plantilla de variables de entorno
├── .gitignore
└── README.md
```

---

## 🔧 Comandos útiles

```bash
# Ver logs de un servicio específico
docker-compose logs -f consumer
docker-compose logs -f simulator

# Detener todos los servicios
docker-compose down

# Detener y eliminar volúmenes (datos)
docker-compose down -v

# Reiniciar un servicio
docker-compose restart streamlit
```

---

## 🛠️ Stack tecnológico

| Categoría        | Tecnología                          |
|------------------|-------------------------------------|
| Streaming        | Apache Kafka (Confluent)            |
| Object Storage   | MinIO                               |
| Base de datos    | PostgreSQL 15                       |
| API              | FastAPI + Uvicorn                   |
| Visualización    | Streamlit, Grafana, Altair          |
| Orquestación     | Docker Compose                      |
| Lenguaje         | Python 3.10 / 3.13                  |

---

## 📌 Notas de diseño

- El **simulador** y el **consumidor** esperan activamente a que Kafka y PostgreSQL estén disponibles antes de iniciar (via `entrypoint.sh` con `nc -z`).
- **MinIO** actúa como data lake ligero; los CSV se almacenan y leen directamente desde Streamlit.
- **PostgreSQL** actúa como DWH; los datos llegan via el consumer de Kafka.
- Las credenciales se gestionan vía archivo `.env` — nunca hardcodeadas en el código.

---

## 👤 Autor

**Balvin** — [GitHub](https://github.com/balvin07)


