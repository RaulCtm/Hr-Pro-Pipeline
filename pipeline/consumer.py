"""
Consumer de Kafka. Responsabilidad ÚNICA: leer mensajes y persistirlos
tal cual en MongoDB.raw_messages con estado 'raw'. NO transforma, NO
valida con Pydantic todavía, NO decide identidad. Eso es trabajo de
identity.py / assembler.py (issue #6).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from pymongo.errors import DuplicateKeyError

from core.database import get_raw_messages_collection

logger = logging.getLogger(__name__)


def build_consumer() -> Consumer:
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id": "hr-pro-pipeline-solo",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit manual, ver mas abajo
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
                # No bloqueamos el pipeline por un mensaje corrupto: se
                # comitea el offset igualmente para no entrar en bucle.
                consumer.commit(message=msg)
                continue

            document = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "payload": payload,
                "status": "raw",
                "received_at": datetime.now(timezone.utc),
            }

            try:
                raw_collection.insert_one(document)
                processed += 1
            except DuplicateKeyError:
                # Redelivery por at-least-once: el indice unico
                # (topic, partition, offset) ya lo tenia guardado.
                duplicates += 1
                logger.debug(
                    "Mensaje duplicado ignorado (topic=%s partition=%s offset=%s)",
                    msg.topic(), msg.partition(), msg.offset(),
                )

            # Commit manual SOLO despues de persistir (o de confirmar que
            # ya estaba persistido). Si el proceso muere antes de esta
            # linea, el mensaje se reprocesa al reiniciar -> idempotente
            # gracias al indice unico de arriba.
            consumer.commit(message=msg)

            if (processed + duplicates) % 100 == 0 and (processed + duplicates) > 0:
                logger.info(
                    "Progreso: %s nuevos, %s duplicados, %s malformados",
                    processed, duplicates, malformed,
                )

    except KeyboardInterrupt:
        logger.info(
            "Consumer detenido por el usuario. Total: %s nuevos, %s duplicados, %s malformados",
            processed, duplicates, malformed,
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_consumer()