"""
Esquemas Pydantic para validación de mensajes de entrada (Issue #5).

Los mensajes que llegan de Kafka están fragmentados (no todos traen los
mismos campos). Este esquema define la estructura esperada del payload,
asegurando que los tipos de datos básicos sean correctos y permitiendo
campos adicionales para no perder información.

La validación estricta de campos obligatorios (como passport) no se hace aquí
porque un fragmento puede no traerlo. La lógica de ensamblado (Issue #6)
se encargará de exigir el passport antes de dar un empleado por completo.
"""

from __future__ import annotations

from typing import Optional, Any

from pydantic import BaseModel, Field, ConfigDict


class KafkaMessagePayload(BaseModel):
    """
    Esquema flexible para validar el payload de los mensajes crudos.
    Basado en los campos reales observados en los logs de MongoDB.
    """
    
    # Permitimos campos extra por si el generador añade nuevos atributos,
    # así no rompemos el pipeline si llega un dato que no tenemos mapeado.
    model_config = ConfigDict(extra='allow', populate_by_name=True)

    # Identificadores y nombres (Bridge keys potenciales para el #6)
    fullname: Optional[str] = None
    name: Optional[str] = None
    last_name: Optional[str] = None
    passport: Optional[str] = None
    
    # Datos de contacto
    email: Optional[str] = None
    telfnumber: Optional[str] = None
    sex: Optional[Any] = None  # En los logs viene como lista (ej. ["ND"]), lo aceptamos flexible

    # Datos de empresa (notar el alias para "company address" que tiene espacio)
    company: Optional[str] = None
    company_address: Optional[str] = Field(default=None, alias="company address")
    company_telfnumber: Optional[str] = None
    company_email: Optional[str] = None
    job: Optional[str] = None

    # Datos bancarios (el generador los manda en mayúsculas, usamos alias)
    iban: Optional[str] = Field(default=None, alias="IBAN")
    salary: Optional[str] = None  # Viene como string con símbolo de moneda, limpieza en #6/#7

    # Datos de ubicación
    address: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None