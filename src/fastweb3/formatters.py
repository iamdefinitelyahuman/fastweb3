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


def normalize_rpc_obj(obj: Any) -> Any:
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
