"""
Script de inicialización de la base de datos PostgreSQL (Issue #7).
Ejecutar una vez para crear el esquema normalizado.
"""

from __future__ import annotations
import pathlib
from core.database import get_postgres_connection

def init_schema() -> None:
    schema_path = pathlib.Path(__file__).parent / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = get_postgres_connection()
    if not conn:
        print("Error: No se pudo conectar a PostgreSQL.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("Esquema normalizado (3FN) verificado/creado en PostgreSQL.")
    except Exception as e:
        print(f"Error al crear el esquema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_schema()