"""Utilidades compartidas de seguridad de datos."""

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