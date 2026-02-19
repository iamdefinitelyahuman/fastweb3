from __future__ import annotations

from typing import Any


def to_int(x: Any) -> int:
    """
    Common JSON-RPC hex quantity formatter.
    Accepts 0x-prefixed hex strings or ints.
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
    if not isinstance(n, int) or n < 0:
        raise ValueError("Quantity must be a non-negative int")
    return hex(n)
