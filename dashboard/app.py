"""
Dashboard de monitorización (Issue #9).

Lee la vista analítica v_employees_complete de PostgreSQL y muestra
KPIs, filtros y gráficos en tiempo real usando Streamlit.
"""

import streamlit as st
import pandas as pd
import re
from core.database import get_postgres_connection

# Configuración de la página
st.set_page_config(page_title="HR Pro Pipeline", page_icon="👥", layout="wide")

@st.cache_data(ttl=10) # Cache de 10 segundos para no saturar Postgres al recargar
def load_data() -> pd.DataFrame:
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()
    try:
        # Leemos directamente de la vista relacional
        query = "SELECT * FROM v_employees_complete;"
        df = pd.read_sql(query, conn)
        
        # Limpiamos el salario (viene como string "161410$" -> float)
        def parse_salary(val):
            if pd.isna(val) or val == "":
                return 0.0
            return float(re.sub(r'[^\d.]', '', str(val)))
            
        df['salary_num'] = df['salary'].apply(parse_salary)
        return df
    except Exception as e:
        st.error(f"Error al leer PostgreSQL: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# Cargar datos
df = load_data()

# --- HEADER ---
st.title("👥 HR Pro Pipeline Dashboard")
st.markdown("Datos curados ensamblando fragmentos de Kafka en tiempo real.")

if df.empty:
    st.warning("No hay datos disponibles en la base de datos. Lanza el pipeline primero.")
    st.stop()

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)

col1.metric("Empleados Totales", len(df))
col2.metric("Empresas Únicas", df['company_name'].nunique())
col3.metric("Ciudades Diferentes", df['city'].nunique())
col4.metric("Salario Medio", f"{df['salary_num'].mean():,.0f} $")

st.divider()

# --- FILTROS ---
st.sidebar.header("🔍 Filtros")
cities = st.sidebar.multiselect("Filtra por Ciudad", options=df['city'].dropna().unique())
companies = st.sidebar.multiselect("Filtra por Empresa", options=df['company_name'].dropna().unique())

filtered_df = df.copy()
if cities:
    filtered_df = filtered_df[filtered_df['city'].isin(cities)]
if companies:
    filtered_df = filtered_df[filtered_df['company_name'].isin(companies)]

# --- GRÁFICOS ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 5 Empresas por Nº de Empleados")
    top_companies = filtered_df['company_name'].value_counts().head(5)
    st.bar_chart(top_companies)

with col_right:
    st.subheader("Distribución de Salarios")
    # Filtramos salarios 0 para que el gráfico tenga sentido
    salaries = filtered_df[filtered_df['salary_num'] > 0]['salary_num']
    if not salaries.empty:
        st.histogram(salaries, bins=20)
    else:
        st.info("No hay datos de salarios para los filtros seleccionados.")

# --- TABLA DE DATOS ---
st.divider()
st.subheader("Detalle de Empleados Ensamblados")
# Seleccionamos columnas legibles para la tabla
columns_to_show = ['fullname', 'personal_email', 'personal_phone', 'city', 'company_name', 'job_title', 'salary']
st.dataframe(filtered_df[columns_to_show].rename(columns={
    'fullname': 'Nombre Completo',
    'personal_email': 'Email Personal',
    'personal_phone': 'Teléfono',
    'city': 'Ciudad',
    'company_name': 'Empresa',
    'job_title': 'Puesto',
    'salary': 'Salario Original'
}), use_container_width=True, hide_index=True)