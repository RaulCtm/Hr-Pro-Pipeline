"""
Consumer de Kafka. Responsabilidad ÚNICA: leer mensajes, validarlos con
Pydantic y persistirlos tal cual en MongoDB.raw_messages con estado 'raw'.
NO resuelve identidad ni ensambla (eso es del #8/#6).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from pymongo.errors import DuplicateKeyError
from pydantic import ValidationError

from core.database import get_raw_messages_collection
from core.schemas import KafkaMessagePayload
from core.utils import mask_pii

logger = logging.getLogger(__name__)


def build_consumer() -> Consumer:
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id": "hr-pro-pipeline-solo",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(config)


def run_consumer() -> None:
    topic = os.getenv("KAFKA_TOPIC", "probando")
    consumer = build_consumer()
    consumer.subscribe([topic])
    raw_collection = get_raw_messages_collection()

    logger.info("Consumer escuchando en topic=%s", topic)

    processed = 0
    duplicates = 0
    malformed = 0
    validation_errors = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Error de Kafka: %s", msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                malformed += 1
                logger.warning(
                    "Mensaje no parseable en partition=%s offset=%s: %s",
                    msg.partition(), msg.offset(), exc,
                )
                consumer.commit(message=msg)
                continue

            # --- NUEVO EN EL #8: Validación con Pydantic ---
            try:
                validated_payload = KafkaMessagePayload.model_validate(payload)
                # Convertimos el modelo validado de vuelta a dict para guardarlo en Mongo
                # Usamos by_alias=True para conservar nombres originales como "company address"
                payload_to_store = validated_payload.model_dump(by_alias=True, exclude_none=True)
            except ValidationError as exc:
                validation_errors += 1
                logger.warning(
                    "Validación Pydantic fallida en partition=%s offset=%s: %s",
                    msg.partition(), msg.offset(), exc,
                )
                # Si no valida, lo descartamos y comiteamos para no atascar Kafka
                consumer.commit(message=msg)
                continue

            document = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "payload": payload_to_store,
                "status": "raw",
                "received_at": datetime.now(timezone.utc),
            }

            try:
                raw_collection.insert_one(document)
                processed += 1
                identifier = (
                    payload_to_store.get("passport")
                    or payload_to_store.get("email")
                    or payload_to_store.get("address")
                    or payload_to_store.get("fullname")
                )
                if identifier:
                    logger.debug(
                        "Mensaje validado y persistido, identificador=%s (partition=%s offset=%s)",
                        mask_pii(str(identifier)),
                        msg.partition(),
                        msg.offset(),
                    )
            except DuplicateKeyError:
                duplicates += 1
                logger.debug(
                    "Mensaje duplicado ignorado (topic=%s partition=%s offset=%s)",
                    msg.topic(), msg.partition(), msg.offset(),
                )

            consumer.commit(message=msg)

            if (processed + duplicates) % 100 == 0 and (processed + duplicates) > 0:
                logger.info(
                    "Progreso: %s nuevos, %s duplicados, %s malformados, %s inválidos",
                    processed, duplicates, malformed, validation_errors,
                )

    except KeyboardInterrupt:
        logger.info(
            "Consumer detenido por el usuario. Total: %s nuevos, %s duplicados, %s malformados, %s inválidos",
            processed, duplicates, malformed, validation_errors,
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    run_consumer()