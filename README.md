# HR Pro Pipeline (versión solo / simplificada)

Pipeline ETL en casi-tiempo-real: **Kafka → MongoDB (raw) → Redis (ensamblaje temporal por bridge keys) → PostgreSQL (curado)**.

Versión individual simplificada del proyecto en equipo. Se elimina deliberadamente:
- Django Admin / API REST → para la demo basta una consulta directa a Postgres (o un Streamlit muy simple).
- Prometheus / Grafana → observabilidad de nivel experto, no aporta a una demo de 10 min.
- Identity promotion / look-back de huérfanos → se simplifica a un único intento de bridge, sin reprocesamiento automático.

Se mantiene:
- **Redis** como caché temporal de ensamblaje (sigue siendo necesario porque un empleado llega en fragmentos: Personal, Location, Professional, Bank, Net — y no siempre en el mismo mensaje).
- **Bridge keys**: `passport` (master key), `fullname` y `address` (bridge keys) — exact match únicamente, sin fuzzy matching.

## Arquitectura

```
Kafka (generador del bootcamp)
   │
   ▼
consumer.py ──► MongoDB.raw_messages (estado: raw)
   │
   ▼
identity.py + assembler.py ──► Redis (TTL, key = passport o fullname/address pendiente de passport)
   │
   ▼  (cuando el registro tiene suficientes campos / TTL vence)
sql_writer.py ──► PostgreSQL.employees (registro curado)
```

## Cadena de identidad (simplificada)

1. **Passport** → master key. Si el mensaje trae passport, crea o actualiza directamente el empleado en Redis/Postgres.
2. **Fullname** → bridge key. Si el mensaje NO trae passport pero sí fullname, se busca en Redis un empleado con ese fullname exacto.
   - 1 coincidencia → se fusiona.
   - 0 o 2+ coincidencias → se descarta como `incomplete` (no se inventa una unión).
3. **Address** → igual que fullname, pero solo aplica para fusionar `Net Data` (se asume que una IP/dirección pertenece a quien ya tiene esa dirección registrada).

> Nota: la promoción retroactiva de huérfanos (re-mirar mensajes antiguos cuando llega un passport nuevo) se deja fuera del alcance solo — está documentada como mejora futura en `docs/architecture.md`.

## Requisitos

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/)
- Docker + Docker Compose
- Acceso al generador de Kafka del bootcamp (broker externo, no se incluye en este compose)

## Puesta en marcha

```bash
cp .env.example .env        # edita KAFKA_BOOTSTRAP_SERVERS y KAFKA_TOPIC
docker compose up -d        # levanta Mongo, Postgres, Redis
uv sync
uv run python sql/init_schema.py   # crea la tabla employees
uv run python -m pipeline.main
```

Comprobación rápida de infraestructura:

```bash
uv run python scripts/check_infra.py
```

## Estructura

```
.
├── core/                # esquemas Pydantic + conexiones a las 3 BBDD + utils (masking)
├── pipeline/             # consumer Kafka, identity resolution, assembler Redis, sql writer
├── sql/                  # DDL de la tabla employees
├── docs/                 # arquitectura + decisiones de diseño
├── scripts/              # check_infra.py
└── tests/
```

## Roadmap (issues del Kanban)

Ver `docs/architecture.md` para el detalle de decisiones y el listado de issues planificado.
