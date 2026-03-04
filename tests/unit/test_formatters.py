import pytest

from fastweb3.formatters import normalize_rpc_obj, to_hex_quantity, to_int


def test_to_int_accepts_hex_str_any_case_and_whitespace() -> None:
    assert to_int("  0x2A ") == 42
    assert to_int("0X2a") == 42


def test_to_int_accepts_decimal_str_and_int() -> None:
    assert to_int("123") == 123
    assert to_int(7) == 7


def test_to_int_rejects_none_and_unsupported_types() -> None:
    with pytest.raises(ValueError, match="Cannot format None"):
        to_int(None)
    with pytest.raises(TypeError, match="Cannot format"):
        to_int({})


def test_to_hex_quantity_requires_non_negative_int() -> None:
    assert to_hex_quantity(0) == "0x0"
    assert to_hex_quantity(255) == "0xff"

    with pytest.raises(ValueError, match="non-negative"):
        to_hex_quantity(-1)
    with pytest.raises(ValueError, match="non-negative"):
        to_hex_quantity("1")  # type: ignore[arg-type]


def test_normalize_rpc_obj_converts_known_hex_quantity_fields_recursively() -> None:
    obj = {
        "gas": "0x5208",
        "nonce": "0x00",
        "notAQuantity": "0x5208",
        "nested": [
            {"blockNumber": "0x10"},
            {"timestamp": "0x0"},
        ],
    }

    out = normalize_rpc_obj(obj)
    assert out["gas"] == 0x5208
    assert out["nonce"] == 0
    assert out["notAQuantity"] == "0x5208"  # untouched
    assert out["nested"][0]["blockNumber"] == 16
    assert out["nested"][1]["timestamp"] == 0
