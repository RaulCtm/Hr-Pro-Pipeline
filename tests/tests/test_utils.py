import pytest
from core.utils import mask_pii, normalize_name

def test_mask_pii_standard():
    assert mask_pii("AB1234567") == "AB*******"

def test_mask_pii_short_string():
    assert mask_pii("AB") == "**"

def test_mask_pii_empty():
    assert mask_pii("") == ""

def test_normalize_name_spaces_and_caps():
    assert normalize_name("  Júlia   Almeida ") == "júlia almeida"

def test_normalize_name_empty():
    assert normalize_name("") == ""