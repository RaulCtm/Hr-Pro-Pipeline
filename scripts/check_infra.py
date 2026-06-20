"""Comprobación rápida de conectividad a Mongo, Postgres y Redis."""

from __future__ import annotations

from core.database import get_mongo_db, get_postgres_connection, get_redis_client


def check_mongo() -> None:
    try:
        get_mongo_db().client.admin.command("ping")
        print("MongoDB: OK")
    except Exception as exc:  # noqa: BLE001
        print(f"MongoDB: FALLO ({exc})")


def check_postgres() -> None:
    try:
        conn = get_postgres_connection()
        conn.close()
        print("PostgreSQL: OK")
    except Exception as exc:  # noqa: BLE001
        print(f"PostgreSQL: FALLO ({exc})")


def check_redis() -> None:
    try:
        get_redis_client().ping()
        print("Redis: OK")
    except Exception as exc:  # noqa: BLE001
        print(f"Redis: FALLO ({exc})")


if __name__ == "__main__":
    check_mongo()
    check_postgres()
    check_redis()
