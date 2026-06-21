"""
Resolución de identidad (Issue #6).

Determina la clave maestra (passport) de un fragmento de mensaje.
Si el fragmento no trae passport, busca en Redis usando bridge keys
(fullname o address) si ya tenemos un passport asociado a ese fragmento.
"""

from __future__ import annotations

import logging
from typing import Optional, Any
from redis import Redis

from core.utils import normalize_name

logger = logging.getLogger(__name__)


def resolve_identity(payload: dict[str, Any], redis_client: Redis) -> Optional[str]:
    """
    Resuelve el passport para un fragmento dado.
    Si lo encuentra, actualiza Redis para enseñarle a otros fragmentos.
    """
    passport = payload.get("passport")
    
    # 1. Si el mensaje trae passport, es la clave maestra directa.
    if passport:
        fullname = payload.get("fullname")
        if not fullname and payload.get("name") and payload.get("last_name"):
            fullname = f"{payload.get('name')} {payload.get('last_name')}"
            
        address = payload.get("address")
        
        if fullname:
            redis_client.set(f"name:{normalize_name(fullname)}", passport)
        if address:
            redis_client.set(f"addr:{normalize_name(address)}", passport)
            
        return passport

    # 2. Si no trae passport, intentamos inferirlo con bridge keys
    fullname = payload.get("fullname")
    if fullname:
        key = f"name:{normalize_name(fullname)}"
        found_passport = redis_client.get(key)
        if found_passport:
            # Decodificación segura (compatibilidad con bytes y str)
            return found_passport.decode('utf-8') if isinstance(found_passport, bytes) else found_passport

    address = payload.get("address")
    if address:
        key = f"addr:{normalize_name(address)}"
        found_passport = redis_client.get(key)
        if found_passport:
            # Decodificación segura (compatibilidad con bytes y str)
            return found_passport.decode('utf-8') if isinstance(found_passport, bytes) else found_passport

    # 3. No se puede resolver la identidad
    logger.debug("Fragmento huérfano sin identidad resoluble: %s", payload)
    return None