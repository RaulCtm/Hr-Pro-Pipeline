# Arquitectura y decisiones de diseño

## Por qué este recorte respecto al proyecto en equipo

| Pieza del proyecto en equipo | Decisión versión solo | Motivo |
|---|---|---|
| Django Admin + DRF API | Fuera | Para una demo de 10 min, una query directa a Postgres (o Streamlit simple) comunica lo mismo en una fracción del tiempo. |
| Prometheus + Grafana | Fuera | Observabilidad "nivel experto"; no aporta a entender el ETL. |
| Identity promotion (look-back de huérfanos) | Fuera (documentado aquí como mejora futura) | Requiere releer Mongo cuando llega un passport nuevo; con el tiempo disponible, un huérfano se queda `incomplete` y punto. |
| Redis (ensamblaje temporal) | **Se mantiene** | Un empleado llega en fragmentos (Personal/Location/Professional/Bank/Net) en mensajes distintos; sin un punto de acumulación temporal no se puede construir el registro completo antes de escribir en Postgres. |
| Bridge keys (fullname, address) | **Se mantiene** | passport no siempre viene en el mismo mensaje que el resto de datos; sin bridge keys se perderían fragmentos. |
| Reintentos con backoff exponencial | Opcional | Si sobra tiempo el lunes. |
| Tests unitarios | Mínimos | Cubrir solo identity.py (la lógica con más riesgo de bugs sutiles). |

## Mejora futura: identity promotion

Si un fragmento llega sin passport y sin bridge key resoluble (0 o 2+ candidatos),
en el proyecto en equipo se guardaba como `soft_orphan` y se reintentaba fusionar
cada vez que llegaba un passport nuevo compatible. En la versión solo, ese
fragmento simplemente expira con el TTL de Redis y queda fuera del registro
final. Para una demo de 10 minutos es una simplificación aceptable y, de
hecho, es un buen punto para mencionar en la presentación ("esto lo
simplifiqué a propósito, en producción haría X").

## Backlog de issues (para crear en GitHub Projects)

### Setup
- [ ] Crear repo + estructura de carpetas + README
- [ ] `docker compose up -d` funcionando (Mongo, Postgres, Redis arriba)
- [ ] Conectar con el generador de Kafka del bootcamp y confirmar que llegan mensajes (sin leer su código)
- [ ] `scripts/check_infra.py` en verde para las 3 bases de datos

### Consumer + Raw
- [ ] Implementar `pipeline/consumer.py`: parseo + insert idempotente en `raw_messages` + commit manual de offset
- [ ] Logging básico con `mask_pii()` para no loguear PII en claro

### Validación
- [ ] Revisar los mensajes reales del generador y mapear cada uno a un esquema de `core/schemas.py` (Personal/Location/Professional/Bank/Net)
- [ ] Documentar aquí qué heurística usaste para inferir el tipo de mensaje si no viene explícito

### Identity + Assembler
- [ ] Implementar `resolve_by_passport` y `resolve_by_bridge_key` en `pipeline/identity.py`
- [ ] Implementar `upsert_fragment` / `is_ready_for_postgres` en `pipeline/assembler.py`
- [ ] Decidir y documentar el criterio de "registro suficientemente completo"

### SQL writer
- [ ] `sql/init_schema.py` ejecutado, tabla `employees` creada
- [ ] Implementar `upsert_employee` con regla de nulos (`COALESCE`) y log de advertencia en conflictos sensibles (iban/salary)

### Integración + demo
- [ ] Cablear `pipeline/main.py` (consumer -> identity -> assembler -> sql_writer)
- [ ] Mini Streamlit (o script) que lea `employees` y muestre empleados completos + contador de mensajes procesados/pendientes
- [ ] Ensayo de la demo en vivo + plan B grabado por si falla la red

### Pulido (si sobra tiempo)
- [ ] Dockerizar el consumer
- [ ] Tests unitarios de `identity.py`
- [ ] Tabla `data_conflicts` para campos sensibles
