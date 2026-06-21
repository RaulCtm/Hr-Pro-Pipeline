"""
Punto de entrada del pipeline. Orquesta:
  consumer (Kafka -> Mongo raw)
  -> identity.resolve_* (decide a quien pertenece el fragmento)
  -> assembler (Redis: acumula fragmentos del mismo empleado)
  -> sql_writer (cuando esta listo, escribe en Postgres)

Para la version solo, puedes empezar ejecutando consumer.py de forma
aislada (issue 1) y conectar el resto progresivamente: no hace falta
que main.py este completo desde el principio.
"""

import logging
from pipeline.consumer import run_consumer

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.info("Iniciando HR Pro Pipeline ETL...")
    run_consumer()

if __name__ == "__main__":
    main()
