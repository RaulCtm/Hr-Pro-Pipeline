"""
Plataforma de Observabilidad de RRHH (Issue #9 + #17) - Multi-Página.

Consume datos de PostgreSQL (empleados) y MongoDB (huérfanos).
Diseñado con un tema corporativo azul armonizado y respeta la encriptación a nivel de BD.
"""

import streamlit as st
import pandas as pd
from core.database import get_postgres_connection, get_raw_messages_collection

# --- CONFIGURACIÓN DE PÁGINA Y TEMA ---
st.set_page_config(page_title="HR Pro Data Platform", layout="wide", initial_sidebar_state="expanded")

# CSS personalizado: Fondo azul, tipografía Inter, bordes armónicos
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
    .stApp {
        background-color: #6674BA;
        font-family: 'Inter', sans-serif;
        color: #FFFFFF;
    }
    section[data-testid="stSidebar"] {
        background-color: #4A55A2; /* Azul un poco más oscuro para diferenciar */
        border-right: 1px solid #1E1B4B;
    }
    /* Tipografía de la sidebar */
    .stSidebar .stMarkdown, .stSidebar label { color: #E0E7FF !important; }
    
    /* Tarjetas KPI */
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 2px solid #1E1B4B; /* Azul marino oscuro en vez de negro puro */
        border-radius: 8px;
        padding: 15px;
        color: #1E1B4B;
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }
    div[data-testid="stMetric"] label {
        color: #4b5563; font-size: 13px; font-weight: 600;
        text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 10px;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #1E1B4B; font-size: 26px; font-weight: 700;
    }
    .section-title {
        font-size: 20px; font-weight: 600; color: #FFFFFF;
        margin-top: 2rem; margin-bottom: 1rem;
    }
    .subtitle { font-size: 16px; color: #E0E7FF; margin-bottom: 2rem; }
    
    /* Indicador en vivo */
    .live-indicator {
        display: flex; align-items: center; gap: 8px;
        background-color: #ffffff; color: #059669;
        padding: 10px 20px; border-radius: 20px;
        font-weight: 700; font-size: 14px; width: fit-content;
        border: 2px solid #1E1B4B;
    }
    .live-dot {
        width: 10px; height: 10px; background-color: #10b981;
        border-radius: 50%; animation: blink 1.5s infinite;
    }
    @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }
    
    /* Contenedores de filtros */
    div[data-testid="stTextInput"], div[data-testid="stSelectbox"] {
        background-color: #ffffff;
        border-radius: 8px;
        border: 2px solid #1E1B4B;
        padding: 5px;
    }
    .stTextInput label, .stSelectbox label, .stRadio label {
        color: #1E1B4B !important; font-weight: 600;
    }
    
    /* Tabla de datos */
    .stDataFrame {
        border: 3px solid #1E1B4B;
        border-radius: 8px;
        overflow: hidden;
        background-color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=5) # Cache corto para ver datos en tiempo real
def load_postgres_data() -> pd.DataFrame:
    conn = get_postgres_connection()
    if not conn: return pd.DataFrame()
    try:
        query = "SELECT * FROM v_employees_complete;"
        df = pd.read_sql(query, conn)
        
        # Como los datos financieros están encriptados (BYTEA), Streamlit no sabe 
        # serializarlos en caché. Los convertimos a booleanos (True/False) aquí mismo.
        if 'iban_encrypted' in df.columns:
            df['has_iban'] = df['iban_encrypted'].notna()
            df = df.drop(columns=['iban_encrypted'])
        if 'salary_encrypted' in df.columns:
            df['has_salary'] = df['salary_encrypted'].notna()
            df = df.drop(columns=['salary_encrypted'])
            
        return df
    except Exception as e:
        st.error(f"Error al leer PostgreSQL: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data(ttl=10)
def load_mongo_orphans() -> pd.DataFrame:
    raw_collection = get_raw_messages_collection()
    orphans = list(raw_collection.find({"status": "orphan"}, {"_id": 0, "payload": 1, "orphan_type": 1, "received_at": 1}))
    if not orphans: return pd.DataFrame()
    
    df = pd.DataFrame(orphans)
    payload_df = df['payload'].apply(pd.Series)
    df = pd.concat([df.drop(['payload'], axis=1), payload_df], axis=1)
    return df

# --- CARGA DE DATOS ---
df_pg = load_postgres_data()
df_orphans = load_mongo_orphans()

# --- NAVEGACIÓN LATERAL ---
st.sidebar.markdown("## Menú de Navegación")
page = st.sidebar.radio("Selecciona una vista:", ["Panel General", "Empleados Completos", "Registro de Huérfanos"])

# --- HEADER ---
col_title, col_live = st.columns([4, 1])
with col_title:
    st.markdown("<h1 style='font-weight: 700; color: #FFFFFF;'>HR Pro Data Platform</h1>", unsafe_allow_html=True)
    st.markdown(f"<p class='subtitle'>Observabilidad y auditoría del pipeline ETL en tiempo real.</p>", unsafe_allow_html=True)
with col_live:
    if not df_pg.empty:
        last_update = df_pg['updated_at'].max().strftime('%H:%M:%S')
        st.markdown(f"""
        <div class="live-indicator">
            <div class="live-dot"></div>
            Pipeline Activo - {last_update}
        </div>
        """, unsafe_allow_html=True)

# --- VISTAS ---
if page == "Panel General":
    if df_pg.empty:
        st.warning("No hay datos en PostgreSQL.")
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("Empleados Ensamblados", len(df_pg))
    with col2: st.metric("Empresas Activas", df_pg['company_name'].nunique())
    with col3: st.metric("Ciudades", df_pg['city'].nunique())
    with col4:
        iban_count = df_pg['has_iban'].sum()
        st.metric("Empleados con IBAN", iban_count)

    st.markdown("<div class='section-title'>Explorador Rápido</div>", unsafe_allow_html=True)
    
    f_col1, f_col2, f_col3, f_col4 = st.columns([3, 2, 2, 1])
    with f_col1: search_query = st.text_input("🔍 Buscar (Nombre, Email, Teléfono, Puesto...)", key="gen_search")
    with f_col2:
        companies = ['Todas'] + sorted(df_pg['company_name'].dropna().unique().tolist())
        selected_company = st.selectbox("Filtrar por Empresa", companies, key="gen_company")
    with f_col3:
        sort_options = {'Nombre Completo': 'fullname', 'Fecha de Actualización': 'updated_at', 'Ciudad': 'city'}
        sort_by = st.selectbox("Ordenar por", list(sort_options.keys()), key="gen_sort")
    with f_col4:
        order_label = st.selectbox("Orden", ['Ascendente', 'Descendente'], key="gen_order")
        ascending = True if order_label == 'Ascendente' else False

    filtered_df = df_pg.copy()
    if search_query:
        mask = (
            filtered_df['fullname'].astype(str).str.contains(search_query, case=False, na=False) |
            filtered_df['personal_email'].astype(str).str.contains(search_query, case=False, na=False) |
            filtered_df['job_title'].astype(str).str.contains(search_query, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    if selected_company != 'Todas':
        filtered_df = filtered_df[filtered_df['company_name'] == selected_company]
    
    filtered_df = filtered_df.sort_values(by=sort_options[sort_by], ascending=ascending, na_position='last')

    columns_to_show = ['fullname', 'personal_email', 'company_name', 'job_title', 'city', 'updated_at']
    st.dataframe(filtered_df[columns_to_show].rename(columns={
        'fullname': 'Nombre', 'personal_email': 'Email', 'company_name': 'Empresa',
        'job_title': 'Puesto', 'city': 'Ciudad', 'updated_at': 'Actualizado'
    }), use_container_width=True, hide_index=True, height=400)


elif page == "Empleados Completos":
    if df_pg.empty:
        st.warning("No hay datos en PostgreSQL.")
        st.stop()

    st.markdown("<div class='section-title'>Empleados Ensamblados (Bridge Keys Resueltas)</div>", unsafe_allow_html=True)
    st.markdown("<p class='subtitle'>Filtrado automático: Empleados con identidad y ubicación resuelta, y al menos un fragmento de empresa/finanzas enlazado.</p>", unsafe_allow_html=True)

    # Lógica para filtrar empleados "completos"
    complete_df = df_pg[
        df_pg['fullname'].notna() & (df_pg['fullname'] != '') &
        df_pg['address'].notna() & (df_pg['address'] != '') &
        df_pg['company_name'].notna() & (df_pg['company_name'] != '')
    ].copy()

    if complete_df.empty:
        st.info("Aún no hay empleados ensamblados. Deja correr el pipeline más tiempo para que lleguen y se enlacen los fragmentos.")
        st.stop()

    col1, col2 = st.columns(2)
    with col1: st.metric("Empleados Ensamblados", len(complete_df))
    with col2:
        iban_count_complete = complete_df['has_iban'].sum()
        st.metric("Con IBAN Registrado", iban_count_complete)

    st.markdown("<div style='height: 2rem;'></div>", unsafe_allow_html=True)

    # Filtros para la tabla de completos
    f_col1, f_col2, f_col3, f_col4 = st.columns([3, 2, 2, 1])
    with f_col1: search_query_c = st.text_input("🔍 Buscar (Nombre, Email, Puesto...)", key="comp_search")
    with f_col2:
        companies_c = ['Todas'] + sorted(complete_df['company_name'].dropna().unique().tolist())
        selected_company_c = st.selectbox("Filtrar por Empresa", companies_c, key="comp_company")
    with f_col3:
        sort_options_c = {'Nombre Completo': 'fullname', 'Ciudad': 'city', 'Fecha de Actualización': 'updated_at'}
        sort_by_c = st.selectbox("Ordenar por", list(sort_options_c.keys()), key="comp_sort")
    with f_col4:
        order_label_c = st.selectbox("Orden", ['Ascendente', 'Descendente'], key="comp_order")
        ascending_c = True if order_label_c == 'Ascendente' else False

    filtered_complete_df = complete_df.copy()
    if search_query_c:
        mask = (
            filtered_complete_df['fullname'].astype(str).str.contains(search_query_c, case=False, na=False) |
            filtered_complete_df['personal_email'].astype(str).str.contains(search_query_c, case=False, na=False) |
            filtered_complete_df['job_title'].astype(str).str.contains(search_query_c, case=False, na=False)
        )
        filtered_complete_df = filtered_complete_df[mask]
    if selected_company_c != 'Todas':
        filtered_complete_df = filtered_complete_df[filtered_complete_df['company_name'] == selected_company_c]

    filtered_complete_df = filtered_complete_df.sort_values(by=sort_options_c[sort_by_c], ascending=ascending_c, na_position='last')

    # --- Data Masking para RRHH (Basado en Encriptación de BD) ---
    display_df = filtered_complete_df.copy()
    # Convertimos los booleanos a 'Sí'/'No' para que se vea bonito en la tabla
    display_df['has_iban'] = display_df['has_iban'].apply(lambda x: 'Sí' if x else 'No')
    display_df['has_salary'] = display_df['has_salary'].apply(lambda x: 'Sí' if x else 'No')

    # Mostramos todos los campos ensamblados
    columns_to_show_c = ['fullname', 'personal_email', 'personal_phone', 'address', 'city', 'country', 'company_name', 'job_title', 'has_iban', 'has_salary', 'updated_at']
    st.dataframe(
        display_df[columns_to_show_c].rename(columns={
            'fullname': 'Nombre Completo', 'personal_email': 'Email', 'personal_phone': 'Teléfono',
            'address': 'Dirección', 'city': 'Ciudad', 'country': 'País', 'company_name': 'Empresa',
            'job_title': 'Puesto', 'has_iban': 'IBAN Registrado', 'has_salary': 'Salario Registrado', 'updated_at': 'Actualizado'
        }),
        use_container_width=True, hide_index=True, height=500
    )


elif page == "Registro de Huérfanos":
    st.markdown("<div class='section-title'>Análisis de Fragmentos Rechazados (MongoDB)</div>", unsafe_allow_html=True)
    
    if df_orphans.empty:
        st.info("No hay huérfanos actualmente. Todos los fragmentos han sido enlazados o el pipeline no ha procesado mensajes aún.")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1: st.metric("Total Huérfanos", len(df_orphans))
    with col2: st.metric("Esperando Passport", len(df_orphans[df_orphans['orphan_type'] == 'Esperando Passport (Tiene Nombre)']))
    with col3: st.metric("Irresolubles", len(df_orphans[df_orphans['orphan_type'] == 'Irresoluble (Sin Identificadores)']))

    st.markdown("<div style='height: 2rem;'></div>", unsafe_allow_html=True)
    
    f_orph1, f_orph2 = st.columns([2, 2])
    with f_orph1:
        orphan_types = ['Todos'] + df_orphans['orphan_type'].dropna().unique().tolist()
        selected_type = st.selectbox("Filtrar por Tipo de Huérfano", orphan_types, key="orph_type")
    with f_orph2:
        orphan_search = st.text_input("🔍 Buscar en payload del huérfano...", key="orph_search")

    filtered_orphans = df_orphans.copy()
    if selected_type != 'Todos':
        filtered_orphans = filtered_orphans[filtered_orphans['orphan_type'] == selected_type]
    
    if orphan_search:
        mask = False
        for col in filtered_orphans.columns:
            if filtered_orphans[col].dtype == 'object':
                mask = mask | filtered_orphans[col].astype(str).str.contains(orphan_search, case=False, na=False)
        filtered_orphans = filtered_orphans[mask]

    st.dataframe(filtered_orphans, use_container_width=True, hide_index=True, height=500)