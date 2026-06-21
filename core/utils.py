"""Utilidades compartidas de seguridad y normalización de datos."""

from __future__ import annotations


def mask_pii(value: str, visible_chars: int = 2) -> str:
    """Enmascara un dato sensible para logs (passport, iban, email...).
    Ej: mask_pii('AB1234567') -> 'AB*******'

    Solo se usa para LOGS, nunca para lo que se persiste en las bases
    de datos (ahi el dato debe quedar siempre completo y sin tocar).
    """
    if not value:
        return ""
    if len(value) <= visible_chars:
        return "*" * len(value)
    return value[:visible_chars] + "*" * (len(value) - visible_chars)


def normalize_name(value: str) -> str:
    """Normaliza un nombre o dirección para usarlo como bridge key en Redis.
    Ej: normalize_name('  Júlia   Almeida ') -> 'julia almeida'
    
    Evita que diferencias de mayúsculas o dobles espacios rompan el enlazamiento.
    """
    if not value:
        return ""
    return " ".join(value.lower().strip().split())