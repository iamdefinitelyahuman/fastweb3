"""Helpers for formatting and normalizing JSON-RPC values."""

from __future__ import annotations

from typing import Any

HEX_QUANTITY_FIELDS = {
    # tx
    "nonce",
    "gas",
    "gasPrice",
    "maxFeePerGas",
    "maxPriorityFeePerGas",
    "value",
    "transactionIndex",
    "type",
    "chainId",
    # receipt
    "cumulativeGasUsed",
    "gasUsed",
    "status",
    "effectiveGasPrice",
    "transactionIndex",
    "logIndex",
    "blockNumber",
    # block
    "number",
    "timestamp",
    "size",
    "gasLimit",
    "gasUsed",
    "difficulty",
    "totalDifficulty",
    "baseFeePerGas",
}


def to_int(x: Any) -> int:
    """Convert a JSON-RPC quantity-like value to an ``int``.

    Args:
        x: An integer or a string. Strings may be ``0x``-prefixed hex or a
            decimal integer representation.

    Returns:
        The parsed integer.

    Raises:
        ValueError: If ``x`` is ``None``.
        TypeError: If ``x`` is not an ``int`` or ``str``.
    """
    if x is None:
        raise ValueError("Cannot format None as int")
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        s = x.strip().lower()
        if s.startswith("0x"):
            return int(s, 16)
        return int(s)
    raise TypeError(f"Cannot format {type(x).__name__} as int")


def to_hex_quantity(n: int) -> str:
    """Convert an integer to a JSON-RPC hex quantity string.

    Args:
        n: Non-negative integer.

    Returns:
        ``0x``-prefixed lowercase hex string.

    Raises:
        ValueError: If ``n`` is not an ``int`` or is negative.
    """
    if not isinstance(n, int) or n < 0:
        raise ValueError("Quantity must be a non-negative int")
    return hex(n)


def normalize_rpc_obj(obj: Any) -> Any:
    """Recursively normalize an RPC response object.

    This function walks lists/dicts and converts known hex-quantity fields to
    integers.

    Args:
        obj: RPC response value.

    Returns:
        Normalized value.
    """
    if isinstance(obj, list):
        return [normalize_rpc_obj(x) for x in obj]

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in HEX_QUANTITY_FIELDS and isinstance(v, str) and v.startswith("0x"):
                out[k] = int(v, 16)
            else:
                out[k] = normalize_rpc_obj(v)
        return out

    return obj
