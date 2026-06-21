import pytest
from pydantic import ValidationError
from core.schemas import KafkaMessagePayload

def test_valid_fullname_fragment():
    msg = {"fullname": "Júlia Almeida", "company": "Acme"}
    p = KafkaMessagePayload.model_validate(msg)
    assert p.fullname == "Júlia Almeida"
    assert p.company == "Acme"

def test_valid_name_last_name_fragment():
    msg = {"name": "Guillaume", "last_name": "Collet", "passport": "123"}
    p = KafkaMessagePayload.model_validate(msg)
    assert p.name == "Guillaume"
    assert p.passport == "123"

def test_iban_alias_mapping():
    msg = {"IBAN": "GB123", "passport": "456"}
    p = KafkaMessagePayload.model_validate(msg)
    assert p.iban == "GB123"

def test_company_address_alias_mapping():
    msg = {"company address": "Calle Falsa 123"}
    p = KafkaMessagePayload.model_validate(msg)
    assert p.company_address == "Calle Falsa 123"

def test_extra_fields_allowed():
    msg = {"fullname": "Test", "random_field": "should_not_break"}
    p = KafkaMessagePayload.model_validate(msg)
    assert p.fullname == "Test"
    # Pydantic v2 almacena los extras en model_extra
    assert p.model_extra.get("random_field") == "should_not_break"

def test_sex_as_list():
    msg = {"sex": ["M"]}
    p = KafkaMessagePayload.model_validate(msg)
    assert p.sex == ["M"]