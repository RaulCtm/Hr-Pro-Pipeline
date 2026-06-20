"""
Ensamblado de fragmentos (Issue #6).

Agrupa los datos de un payload bajo la clave maestra (passport) en Redis.
Usa un Hash de Redis por empleado para ir acumulando los campos sin perder
los que ya teníamos.
"""

from __future__ import annotations

import logging
from typing import Any
from redis import Redis

logger = logging.getLogger(__name__)


def assemble_employee(passport: str, payload: dict[str, Any], redis_client: Redis) -> dict[str, Any]:
    """
    Fusiona un payload en el Hash de Redis del empleado.
    Devuelve el estado actual del empleado ensamblado.
    """
    redis_key = f"emp:{passport}"
    
    # Preparamos los campos a insertar (Redis no acepta valores None)
    fields_to_set = {}
    for k, v in payload.items():
        if v is not None:
            # Si sex viene como lista (ej. ["M"]), lo convertimos a string
            if isinstance(v, list):
                v = ",".join(map(str, v))
            fields_to_set[k] = str(v)
            
    if fields_to_set:
        redis_client.hset(redis_key, mapping=fields_to_set)
        logger.debug("Fragmento ensamblado bajo %s. Campos añadidos: %s", redis_key, list(fields_to_set.keys()))

    # Leemos el estado actual completo del empleado en Redis
    assembled_data = redis_client.hgetall(redis_key)
    
    # Decodificamos de forma segura (compatibilidad con bytes y str)
    result = {}
    for k, v in assembled_data.items():
        k_str = k.decode('utf-8') if isinstance(k, bytes) else k
        v_str = v.decode('utf-8') if isinstance(v, bytes) else v
        result[k_str] = v_str
        
    return result