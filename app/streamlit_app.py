import streamlit as st
import pandas as pd
from minio import Minio
import io
import altair as alt
import requests
import psycopg2

# -------------------------------
# 🔧 Configuración de MinIO
# -------------------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET_NAME = "air-quality-data"

# -------------------------------
# 🧠 Cliente MinIO
# -------------------------------
try:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    st.sidebar.success("✅ Conexión con MinIO exitosa")
except Exception as e:
    st.sidebar.error(f"❌ Error de conexión con MinIO: {e}")

# -------------------------------
# 🎛️ Barra lateral
# -------------------------------
st.sidebar.header("⚙️ Generar datos simulados")

if st.sidebar.button("📡 Enviar datos a MinIO"):
    with st.spinner("Enviando datos a FastAPI..."):
        try:
            response = requests.post("http://api:8000/test_minio")
            if response.status_code == 200 and response.json().get("status") == "ok":
                st.sidebar.success(f"✅ Archivo generado: {response.json()['filename']}")
                st.rerun()
            else:
                st.sidebar.error(f"❌ Error: {response.json().get('detail', 'Sin detalle')}")
        except Exception as e:
            st.sidebar.error(f"❌ No se pudo conectar con FastAPI: {e}")

if st.sidebar.button("🔄 Recargar datos"):
    st.rerun()

# -------------------------------
# 📥 Cargar datos desde MinIO
# -------------------------------
def load_all_data():
    try:
        objects = client.list_objects(BUCKET_NAME, recursive=True)
        dfs = []
        for obj in objects:
            if obj.object_name.endswith(".csv"):
                response = client.get_object(BUCKET_NAME, obj.object_name)
                df = pd.read_csv(io.BytesIO(response.read()))
                dfs.append(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al cargar datos: {e}")
        return pd.DataFrame()

# -------------------------------
# 🔌 Conexión a PostgreSQL
# -------------------------------
def load_postgres_data():
    try:
        conn = psycopg2.connect(
            host="postgres-dwh",
            port=5432,
            dbname="air_quality_dwh",
            user="airquality",
            password="airquality123"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM air_quality_data ORDER BY timestamp DESC LIMIT 100;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df_pg = pd.DataFrame(rows, columns=columns)
        cursor.close()
        conn.close()
        return df_pg
    except Exception as e:
        st.error(f"❌ Error al conectar con PostgreSQL: {e}")
        return pd.DataFrame()

# -------------------------------
# 🖥️ Interfaz principal
# -------------------------------
st.set_page_config(page_title="Calidad del Aire IoT", layout="wide")
st.title("📊 Dashboard de Calidad del Aire")

df = load_all_data()

if df.empty:
    st.warning("⚠️ No hay datos disponibles en el bucket.")
else:
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

    if "station_id" in df.columns:
        station_ids = df["station_id"].unique()
        selected_station = st.selectbox("🏷️ Filtrar por estación", station_ids)
        df = df[df["station_id"] == selected_station]

    st.subheader("📋 Datos crudos")
    st.dataframe(df, use_container_width=True)

    st.subheader("📌 Última medición")
    latest = df.iloc[-1]
    cols = st.columns(5)
    cols[0].metric("🌡️ Temp (°C)", f"{latest['temperature']:.1f}")
    cols[1].metric("💧 Humedad (%)", f"{latest['humidity']:.1f}")
    cols[2].metric("🫁 CO₂ (ppm)", f"{latest['co2']:.0f}")
    cols[3].metric("🌫️ PM2.5", f"{latest['pm25']:.1f}")
    cols[4].metric("🌫️ PM10", f"{latest.get('pm10', 0):.1f}")

    st.subheader("📈 Evolución temporal")
    for col in ["temperature", "humidity", "co2", "pm25", "pm10"]:
        if col in df.columns:
            st.markdown(f"**{col.capitalize()}**")
            chart = alt.Chart(df).mark_line().encode(
                x='timestamp:T',
                y=f'{col}:Q'
            ).properties(height=200)
            st.altair_chart(chart, use_container_width=True)

    st.subheader("📊 Estadísticas generales")
    st.write(df.describe())

# -------------------------------
# 🧭 Navegación por pestañas
# -------------------------------
tab1, tab2 = st.tabs(["📁 MinIO", "🗄️ PostgreSQL"])

with tab1:
    st.header("📁 Datos desde MinIO")
    # Ya cargados arriba

with tab2:
    st.header("🗄️ Datos desde PostgreSQL")
    df_pg = load_postgres_data()
    if df_pg.empty:
        st.warning("No hay datos disponibles en PostgreSQL.")
    else:
        st.dataframe(df_pg, use_container_width=True)

        st.subheader("📈 Evolución temporal")
        if "timestamp" in df_pg.columns:
            df_pg["timestamp"] = pd.to_datetime(df_pg["timestamp"])
            df_pg = df_pg.sort_values("timestamp")

            for col in ["temperature", "humidity", "co2", "pm25"]:
                if col in df_pg.columns:
                    st.markdown(f"**{col.capitalize()}**")
                    chart = alt.Chart(df_pg).mark_line().encode(
                        x='timestamp:T',
                        y=f'{col}:Q'
                    ).properties(height=200)
                    st.altair_chart(chart, use_container_width=True)

        st.subheader("📊 Estadísticas generales")
        st.write(df_pg.describe())
