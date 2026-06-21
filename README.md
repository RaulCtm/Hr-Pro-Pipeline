# HR Pro Pipeline

HR Pro Pipeline es un pipeline ETL en tiempo real diseñado para el procesamiento y ensamblaje de datos de recursos humanos fragmentados. Su objetivo principal es consumir mensajes caóticos provenientes de Apache Kafka, resolver identidades en una capa de caché de alto rendimiento y persistir los datos estructurados, curados y seguros en una base de datos relacional.

---

## Arquitectura del Sistema

El flujo de datos se procesa de forma secuencial y asíncrona a través de los siguientes componentes:


```

Kafka ──> Pydantic (Validación) ──> MongoDB (Raw) ──> Redis (Ensamblaje) ──> PostgreSQL (Curado) ──> Streamlit & Grafana (Visualización)

```

---

## Características Técnicas Principales

### Ingesta Idempotente
* **Control de offsets:** Commit manual de offsets en Kafka posterior a la persistencia.
* **Evitación de duplicados:** Uso de índices únicos en MongoDB para omitir el procesamiento ETL repetido en caso de reinicios o redeliveries de la red.

### Resolución de Identidad (Bridge Keys)
* **Índice inverso:** Uso de Redis para enlazar fragmentos de mensajes que no comparten el mismo identificador maestro (`passport`), utilizando claves puente (`fullname` y `address`).
* **Dead Letter Queue (DLQ):** Clasificación automática de mensajes no resolubles etiquetados en MongoDB como huérfanos para su posterior auditoría.

### Seguridad de Datos (Defense in Depth)
* **Pseudonimización:** El campo `passport` se almacena transformado mediante hashing criptográfico SHA-256.
* **Surrogate Keys:** Uso de identificadores UUID generados por la base de datos (mediante la extensión `pgcrypto`) como Claves Primarias, aislando el PII de las tablas de detalle.
* **Column-Level Encryption:** Los datos sensibles financieros (`iban` y `salary`) se encriptan de forma simétrica directamente en PostgreSQL mediante `pgp_sym_encrypt`.
* **Least Privilege:** El cuadro de mando de recursos humanos no dispone de las llaves de desencriptación; restringe su acceso a la verificación lógica de la existencia del dato (Verdadero/Falso).

### Observabilidad y Calidad de Datos
* **Métricas en tiempo real:** Instrumentación del consumidor con `prometheus_client` para la exposición de métricas operativas (throughput, duplicados, malformados, ensamblados y huérfanos).
* **Esquemas resilientes:** Validación con Pydantic v2 configurado con `extra='allow'` para tolerar la adición de nuevas propiedades por parte del generador sin interrumpir el flujo.

---

## Stack Tecnológico

* **Lenguaje de Programación:** Python 3.12 (Librerías principales: Pydantic, pymongo, redis, psycopg2, prometheus-client)
* **Mensajería y Almacenamiento:** Apache Kafka, MongoDB, Redis, PostgreSQL (con extensión pgcrypto)
* **Infraestructura y Monitoreo:** Docker, Docker Compose, Prometheus, Grafana
* **Capa de Presentación:** Streamlit, Plotly

---

## Estructura del Proyecto

```text
hr-pro-pipeline/
├── core/               # Conexiones a bases de datos, esquemas Pydantic y utilidades de sanitización
├── pipeline/           # Componentes del proceso ETL (consumer, identity, assembler, sql_writer, main)
├── sql/                # Definición del esquema DDL de PostgreSQL y scripts de inicialización
├── dashboard/          # Aplicación Streamlit (Panel de visualización multi-página)
├── tests/              # Batería de pruebas unitarias basadas en pytest
├── docker-compose.yml  # Definición de la infraestructura contenerizada
├── prometheus.yml      # Configuración de los intervalos de muestreo para métricas
└── pyproject.toml      # Configuración del proyecto y gestión de dependencias

```

---

## Instrucciones de Ejecución

### 1. Levantar la Infraestructura

Inicie los servicios de bases de datos, mensajería y monitoreo:

```bash
docker compose up -d

```

### 2. Inicializar el Esquema de la Base de Datos SQL

Cree las tablas, extensiones y vistas analíticas necesarias en PostgreSQL:

```bash
export PYTHONPATH=.
uv run python sql/init_schema.py

```

### 3. Iniciar el Pipeline ETL

Ejecute el orquestador principal del consumidor para comenzar a procesar mensajes:

```bash
uv run python pipeline/main.py

```

### 4. Desplegar el Dashboard de Recursos Humanos

Inicie la interfaz gráfica multi-página de Streamlit:

```bash
uv run streamlit run dashboard/app.py

```

### 5. Configuración de la Monitorización (Grafana)

* Acceda a la interfaz web: `http://localhost:3000` (Credenciales por defecto: `admin` / `admin`).
* Añada una nueva fuente de datos (Data Source) seleccionando **Prometheus**.
* Configure la URL del servidor como: `http://prometheus:9090`.
