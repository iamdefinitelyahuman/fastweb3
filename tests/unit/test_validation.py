from __future__ import annotations

import pytest

from fastweb3 import validation
from fastweb3.errors import ValidationError

# ----------------------------
# helpers
# ----------------------------

UINT256_MAX = 2**256 - 1


# ----------------------------
# normalize_address
# ----------------------------


def test_normalize_address_strict_accepts_any_case_and_lowercases() -> None:
    a = "0xAaBbCcDdEeFf00112233445566778899AaBbCcDd"
    assert validation.normalize_address(a, strict=True) == a.lower()


def test_normalize_address_strict_rejects_missing_0x() -> None:
    with pytest.raises(ValidationError, match="0x-prefixed"):
        validation.normalize_address("a" * 40, strict=True)


def test_normalize_address_strict_rejects_wrong_length() -> None:
    with pytest.raises(ValidationError, match="20 bytes"):
        validation.normalize_address("0x" + "aa" * 19, strict=True)


def test_normalize_address_strict_rejects_non_hex() -> None:
    with pytest.raises(ValidationError, match="hex"):
        validation.normalize_address("0x" + ("gg" * 20), strict=True)


def test_normalize_address_bytes_strict_requires_len_20() -> None:
    assert validation.normalize_address(b"\x11" * 20, strict=True) == "0x" + ("11" * 20)
    with pytest.raises(ValidationError, match="length 20"):
        validation.normalize_address(b"\x11" * 19, strict=True)


def test_normalize_address_non_strict_passthrough_for_str() -> None:
    a = " 0xABCDEF "  # not a real address, but non-strict should not police
    assert validation.normalize_address(a, strict=False) == "0xABCDEF"


def test_normalize_address_non_strict_bytes_are_hexified() -> None:
    assert validation.normalize_address(b"\x00\x01", strict=False) == "0x0001"


# ----------------------------
# hash32
# ----------------------------


def test_hash32_strict_accepts_and_lowercases() -> None:
    h = "0x" + ("Aa" * 32)
    assert validation.hash32(h, name="tx_hash", strict=True) == "0x" + ("aa" * 32)


def test_hash32_strict_rejects_wrong_length() -> None:
    with pytest.raises(ValidationError, match="32 bytes"):
        validation.hash32("0x" + ("aa" * 31), name="block_hash", strict=True)


def test_hash32_strict_rejects_non_hex() -> None:
    with pytest.raises(ValidationError, match="hex"):
        validation.hash32("0x" + ("gg" * 32), name="tx_hash", strict=True)


def test_hash32_bytes_strict_requires_len_32() -> None:
    assert validation.hash32(b"\x22" * 32, name="h", strict=True) == "0x" + ("22" * 32)
    with pytest.raises(ValidationError, match="length 32"):
        validation.hash32(b"\x22" * 31, name="h", strict=True)


def test_hash32_non_strict_passthrough_for_str() -> None:
    s = " not even hex "
    assert validation.hash32(s, name="h", strict=False) == "not even hex"


# ----------------------------
# data_hex
# ----------------------------


def test_data_hex_strict_accepts_even_length_and_lowercases() -> None:
    assert validation.data_hex("0xAAbb", name="data", strict=True) == "0xaabb"


def test_data_hex_strict_allows_empty_0x_when_allow_empty() -> None:
    assert validation.data_hex("0x", name="data", strict=True, allow_empty=True) == "0x"


def test_data_hex_strict_rejects_empty_when_disallowed() -> None:
    with pytest.raises(ValidationError, match="cannot be empty"):
        validation.data_hex("0x", name="signed_tx", strict=True, allow_empty=False)


def test_data_hex_strict_rejects_odd_length() -> None:
    with pytest.raises(ValidationError, match="even length"):
        validation.data_hex("0xabc", name="data", strict=True)


def test_data_hex_strict_rejects_non_hex() -> None:
    with pytest.raises(ValidationError, match="hex"):
        validation.data_hex("0xzz", name="data", strict=True)


def test_data_hex_bytes_strict_hexifies_and_enforces_empty_rule() -> None:
    assert validation.data_hex(b"\x01\x02", name="data", strict=True) == "0x0102"
    assert validation.data_hex(b"", name="data", strict=True, allow_empty=True) == "0x"
    with pytest.raises(ValidationError, match="cannot be empty"):
        validation.data_hex(b"", name="signed_tx", strict=True, allow_empty=False)


def test_data_hex_non_strict_passthrough_for_str() -> None:
    assert validation.data_hex(" potato ", name="data", strict=False) == "potato"


# ----------------------------
# quantity / index
# ----------------------------


def test_quantity_int_strict_bounds() -> None:
    assert validation.quantity(0, strict=True) == "0x0"
    assert validation.quantity(UINT256_MAX, strict=True) == hex(UINT256_MAX)

    with pytest.raises(ValidationError, match="0 <= n <= 2\\*\\*256-1"):
        validation.quantity(-1, strict=True)

    with pytest.raises(ValidationError, match="0 <= n <= 2\\*\\*256-1"):
        validation.quantity(UINT256_MAX + 1, strict=True)


def test_quantity_strict_requires_0x_and_hex_and_bounds() -> None:
    assert validation.quantity("0x0", strict=True) == "0x0"
    assert validation.quantity("0x01", strict=True) == "0x01"
    assert validation.quantity("0xA", strict=True) == "0xa"

    with pytest.raises(ValidationError, match="0x-prefixed"):
        validation.quantity("123", strict=True)

    with pytest.raises(ValidationError, match="cannot be empty"):
        validation.quantity("0x", strict=True)

    with pytest.raises(ValidationError, match="must be hex"):
        validation.quantity("0xzz", strict=True)

    # overflow (2**256)
    with pytest.raises(ValidationError, match="<= 2\\*\\*256-1"):
        validation.quantity("0x" + ("1" + "0" * 64), strict=True)  # 0x1_00..00 (257 bits)


def test_quantity_non_strict_passthrough_for_str() -> None:
    assert validation.quantity(" potato ", strict=False) == "potato"


def test_index_is_quantity() -> None:
    assert validation.index(5, strict=True) == "0x5"
    assert validation.index("0x2a", strict=True) == "0x2a"


# ----------------------------
# block_id
# ----------------------------


def test_block_id_strict_tags_lowercased() -> None:
    assert validation.block_id("LATEST", strict=True) == "latest"
    assert validation.block_id("finalized", strict=True) == "finalized"


def test_block_id_strict_int_to_hex() -> None:
    assert validation.block_id(123, strict=True) == "0x7b"
    with pytest.raises(ValidationError, match=">= 0"):
        validation.block_id(-1, strict=True)


def test_block_id_strict_non_tag_requires_quantity_hex() -> None:
    assert validation.block_id("0x10", strict=True) == "0x10"
    with pytest.raises(ValidationError, match="0x-prefixed"):
        validation.block_id("nope", strict=True)


def test_block_id_non_strict_passthrough() -> None:
    assert validation.block_id("  banana ", strict=False) == "banana"


# ----------------------------
# topics
# ----------------------------


def test_topics_strict_validates_hashes_and_nested_or() -> None:
    t0 = "0x" + ("11" * 32)
    t1 = b"\x22" * 32
    t2 = "0x" + ("33" * 32)

    out = validation.topics([t0, None, [t1, t2]], strict=True)
    assert out == [
        t0,
        None,
        ["0x" + ("22" * 32), t2],
    ]


def test_topics_strict_rejects_bad_shapes() -> None:
    with pytest.raises(ValidationError, match="topics must be list"):
        validation.topics("nope", strict=True)  # type: ignore[arg-type]

    with pytest.raises(ValidationError, match="topic\\[0\\]"):
        validation.topics([123], strict=True)  # type: ignore[list-item]

    with pytest.raises(ValidationError, match="topic\\[0\\]\\[0\\]"):
        validation.topics([["nope"]], strict=True)


def test_topics_non_strict_passthrough() -> None:
    raw = ["whatever", None, ["still", "whatever"]]
    assert validation.topics(raw, strict=False) == raw


# ----------------------------
# tx/filter invariants
# ----------------------------


def test_validate_tx_object_strict_rejects_gasprice_with_1559_fields() -> None:
    tx = {"gasPrice": "0x1", "maxFeePerGas": "0x2", "maxPriorityFeePerGas": "0x1"}
    with pytest.raises(ValidationError, match="gasPrice"):
        validation.validate_tx_object(tx, strict=True)


def test_validate_tx_object_strict_requires_both_1559_fields() -> None:
    tx1 = {"maxFeePerGas": "0x2"}
    tx2 = {"maxPriorityFeePerGas": "0x1"}
    with pytest.raises(ValidationError, match="both maxFeePerGas and maxPriorityFeePerGas"):
        validation.validate_tx_object(tx1, strict=True)
    with pytest.raises(ValidationError, match="both maxFeePerGas and maxPriorityFeePerGas"):
        validation.validate_tx_object(tx2, strict=True)


def test_validate_tx_object_non_strict_noop() -> None:
    tx = {"gasPrice": "0x1", "maxFeePerGas": "0x2"}  # nonsense, but strict=False should ignore
    validation.validate_tx_object(tx, strict=False)


def test_validate_filter_object_strict_rejects_blockhash_with_from_to() -> None:
    flt = {"blockHash": "0x" + ("11" * 32), "fromBlock": "latest"}
    with pytest.raises(ValidationError, match="blockHash"):
        validation.validate_filter_object(flt, strict=True)


def test_validate_filter_object_non_strict_noop() -> None:
    flt = {"blockHash": "nope", "fromBlock": "also nope"}
    validation.validate_filter_object(flt, strict=False)
