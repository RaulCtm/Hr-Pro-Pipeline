"""
Conexiones centralizadas a las 3 bases de datos. Un único punto de
configuración para que el consumer, el assembler y el sql_writer no
dupliquen lógica de conexión.
"""

from __future__ import annotations

import os

import psycopg2
import redis
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

load_dotenv()


def get_mongo_db() -> Database:
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    auth_source = os.getenv("MONGO_AUTH_SOURCE", "admin")
    db_name = os.getenv("MONGO_DB", "hr_pro")

    if user and password:
        uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource={auth_source}"
    else:
        uri = f"mongodb://{host}:{port}/"

    client = MongoClient(uri)
    return client[db_name]


def get_raw_messages_collection() -> Collection:
    db = get_mongo_db()
    collection = db["raw_messages"]
    # Idempotencia: un mismo mensaje de Kafka (topic, partition, offset) no se
    # procesa dos veces aunque el consumer se reinicie.
    collection.create_index(
        [("topic", 1), ("partition", 1), ("offset", 1)], unique=True
    )
    return collection


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "hr_pro"),
        user=os.getenv("POSTGRES_USER", "hr_user"),
        password=os.getenv("POSTGRES_PASSWORD", "hr_password"),
    )
