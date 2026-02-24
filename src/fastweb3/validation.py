from __future__ import annotations

from typing import Any, Mapping

from .errors import ValidationError

_HEX_CHARS = set("0123456789abcdefABCDEF")
_ALLOWED_BLOCK_TAGS = {"latest", "pending", "earliest", "safe", "finalized"}

UINT256_MAX = 2**256 - 1


def _is_hex(s: str) -> bool:
    return all(c in _HEX_CHARS for c in s)


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ValidationError(msg)


def normalize_address(addr: str | bytes, *, strict: bool) -> str:
    """
    Accepts:
      - str: 0x + 40 hex chars (any case). Does NOT enforce EIP-55 checksum.
      - bytes: length 20
    Returns:
      - normalized 0x + lowercase hex in strict mode
      - passthrough (stripped) in non-strict mode for str inputs
    """
    if isinstance(addr, bytes):
        if not strict:
            return "0x" + addr.hex()
        _require(len(addr) == 20, "address bytes must be length 20")
        return "0x" + addr.hex()

    _require(isinstance(addr, str), f"address must be str|bytes, got {type(addr).__name__}")
    a = addr.strip()
    if not strict:
        return a

    _require(a.startswith(("0x", "0X")), "address must be 0x-prefixed")
    _require(len(a) == 42, "address must be 20 bytes (40 hex chars)")
    _require(_is_hex(a[2:]), "address must be hex")
    return "0x" + a[2:].lower()


def hash32(x: str | bytes, *, name: str, strict: bool) -> str:
    """
    Accepts:
      - str: 0x + 64 hex chars
      - bytes: length 32
    Returns lowercase 0x-hex in strict mode; passthrough (stripped) in non-strict mode for str input
    """
    if isinstance(x, bytes):
        if not strict:
            return "0x" + x.hex()
        _require(len(x) == 32, f"{name} bytes must be length 32")
        return "0x" + x.hex()

    _require(isinstance(x, str), f"{name} must be str|bytes, got {type(x).__name__}")
    s = x.strip()
    if not strict:
        return s

    _require(s.startswith(("0x", "0X")), f"{name} must be 0x-prefixed")
    _require(len(s) == 66, f"{name} must be 32 bytes (64 hex chars)")
    _require(_is_hex(s[2:]), f"{name} must be hex")
    return "0x" + s[2:].lower()


def data_hex(x: str | bytes, *, name: str, strict: bool, allow_empty: bool = True) -> str:
    """
    Accepts:
      - str: 0x-prefixed even-length hex (empty allowed if allow_empty)
      - bytes: any length (empty allowed if allow_empty)

    Returns lowercase 0x-hex in strict mode; passthrough (stripped) in non-strict mode for str input
    """
    if isinstance(x, bytes):
        if not x and allow_empty:
            return "0x"
        _require(len(x) > 0, f"{name} cannot be empty")
        return "0x" + x.hex()

    _require(isinstance(x, str), f"{name} must be str|bytes, got {type(x).__name__}")
    s = x.strip()
    if not strict:
        return s

    _require(s.startswith(("0x", "0X")), f"{name} must be 0x-prefixed")

    body = s[2:]
    if not body:
        if allow_empty:
            return "0x"
        raise ValidationError(f"{name} cannot be empty")

    _require(len(body) % 2 == 0, f"{name} hex must have even length")
    _require(_is_hex(body), f"{name} must be hex")

    return "0x" + body.lower()


def quantity(x: int | str, *, strict: bool) -> str:
    """
    JSON-RPC quantity:
      - int -> 0x hex (must be 0 <= n <= 2**256-1)
      - str -> 0x-prefixed hex (validated in strict mode), bound-checked to uint256 in strict mode
    """
    if isinstance(x, int):
        _require(0 <= x <= UINT256_MAX, "quantity ints must be 0 <= n <= 2**256-1")
        return hex(x)

    _require(isinstance(x, str), f"quantity must be int|str, got {type(x).__name__}")
    s = x.strip()
    if not strict:
        return s

    _require(s.startswith(("0x", "0X")), "quantity strings must be 0x-prefixed")
    body = s[2:]
    _require(body != "", "quantity string cannot be empty '0x'")
    _require(_is_hex(body), "quantity must be hex")

    n = int(body, 16)
    _require(n <= UINT256_MAX, "quantity must be <= 2**256-1")

    # Do not enforce minimal encoding (leading zeros). Nodes vary; it's not worth the annoyance.
    return "0x" + body.lower()


def index(x: int | str, *, strict: bool) -> str:
    """Same validation rules as quantity()."""
    return quantity(x, strict=strict)


def block_id(x: int | str, *, strict: bool) -> str:
    """
    Block parameter:
      - int >= 0 -> hex quantity
      - str tag -> latest|pending|earliest|safe|finalized (strict mode)
      - str hex quantity -> validated in strict mode
    """
    if isinstance(x, int):
        _require(x >= 0, "block number must be >= 0")
        return hex(x)

    _require(isinstance(x, str), f"block id must be int|str, got {type(x).__name__}")
    s = x.strip()
    if not strict:
        return s

    sl = s.lower()
    if sl in _ALLOWED_BLOCK_TAGS:
        return sl

    return quantity(s, strict=True)


def topics(
    topics: list[str | bytes | list[str | bytes] | None] | None, *, strict: bool
) -> list[str | list[str] | None] | None:
    """
    Filter topics:
      - list entries can be:
          - None
          - topic hash32 (str|bytes)
          - list of topic hash32 (OR semantics)
    In strict mode, all hashes are validated as 32-byte values.
    """
    if topics is None:
        return None
    _require(isinstance(topics, list), f"topics must be list, got {type(topics).__name__}")

    if not strict:
        # Preserve user-provided shape; no coercion.
        return topics  # type: ignore[return-value]

    out: list[str | list[str] | None] = []
    for i, t in enumerate(topics):
        if t is None:
            out.append(None)
        elif isinstance(t, (str, bytes)):
            out.append(hash32(t, name=f"topic[{i}]", strict=True))
        elif isinstance(t, list):
            inner: list[str] = []
            for j, tt in enumerate(t):
                _require(isinstance(tt, (str, bytes)), f"topic[{i}][{j}] must be str|bytes")
                inner.append(hash32(tt, name=f"topic[{i}][{j}]", strict=True))
            out.append(inner)
        else:
            raise ValidationError(f"topic[{i}] must be str|bytes|list[str|bytes]|None")
    return out


def validate_tx_object(tx: Mapping[str, Any], *, strict: bool) -> None:
    """
    Validate invariants on a JSON-RPC "transaction object" dict.

    This is NOT method-specific ("eth_call" vs "eth_sendTransaction") —
    it's the generic object shape rules that are basically always user error when violated.
    """
    if not strict:
        return

    has_gas_price = "gasPrice" in tx
    has_max_fee = "maxFeePerGas" in tx
    has_max_priority = "maxPriorityFeePerGas" in tx

    if has_gas_price and (has_max_fee or has_max_priority):
        raise ValidationError(
            "Transaction object cannot specify gasPrice together with "
            "maxFeePerGas/maxPriorityFeePerGas"
        )

    # If user opts into 1559 fields, require both. (Otherwise it's usually a bug.)
    if has_max_fee ^ has_max_priority:
        raise ValidationError(
            "Transaction object must specify both maxFeePerGas and maxPriorityFeePerGas "
            "when using EIP-1559 fee fields"
        )


def validate_filter_object(flt: Mapping[str, Any], *, strict: bool) -> None:
    """
    Validate invariants on a JSON-RPC filter object dict.
    """
    if not strict:
        return

    if "blockHash" in flt and ("fromBlock" in flt or "toBlock" in flt):
        raise ValidationError("Filter object cannot combine blockHash with fromBlock/toBlock")
