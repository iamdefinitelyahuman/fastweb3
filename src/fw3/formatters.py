"""Helpers for formatting and normalizing JSON-RPC values."""

from __future__ import annotations

from typing import Any, Mapping

from . import validation

HEX_QUANTITY_FIELDS = {
    # tx / call / proof
    "nonce",
    "gas",
    "gasPrice",
    "maxFeePerGas",
    "maxPriorityFeePerGas",
    "value",
    "transactionIndex",
    "type",
    "chainId",
    "balance",
    # receipt
    "cumulativeGasUsed",
    "gasUsed",
    "status",
    "effectiveGasPrice",
    "transactionIndex",
    "logIndex",
    "blockNumber",
    # block / fee history
    "number",
    "timestamp",
    "size",
    "gasLimit",
    "gasUsed",
    "difficulty",
    "totalDifficulty",
    "baseFeePerGas",
    "oldestBlock",
    "reward",
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


def _normalize_quantity_value(x: Any) -> Any:
    """Recursively normalize quantity-like values inside a known quantity field."""
    if isinstance(x, str) and x.startswith("0x"):
        return int(x, 16)
    if isinstance(x, list):
        return [_normalize_quantity_value(v) for v in x]
    return x


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
            if k in HEX_QUANTITY_FIELDS:
                out[k] = _normalize_quantity_value(v)
            else:
                out[k] = normalize_rpc_obj(v)
        return out

    return obj


def build_transaction_object(
    *,
    from_: str | bytes | None = None,
    to: str | bytes | None = None,
    gas: int | str | None = None,
    gas_price: int | str | None = None,
    max_fee_per_gas: int | str | None = None,
    max_priority_fee_per_gas: int | str | None = None,
    value: int | str | None = None,
    data: str | bytes | None = None,
    nonce: int | str | None = None,
    chain_id: int | str | None = None,
    type_: int | str | None = None,
    access_list: list[Mapping[str, Any]] | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """Build a normalized transaction object from explicit parameters.

    This function accepts Python-native transaction parameters and converts them
    into a canonical JSON-RPC transaction dict suitable for use with Ethereum
    methods such as ``eth_call``, ``eth_estimateGas`` and ``eth_sendTransaction``.

    All provided fields are validated and normalized according to Ethereum RPC
    requirements. Fields that are ``None`` are omitted from the resulting object.

    Args:
        from_ (str | bytes | None):
            Sender address.

        to (str | bytes | None):
            Recipient address. If omitted, the transaction is treated as a contract
            deployment.

        gas (int | str | None):
            Gas limit.

        gas_price (int | str | None):
            Legacy gas price (pre-EIP-1559).

        max_fee_per_gas (int | str | None):
            Maximum total fee per gas (EIP-1559).

        max_priority_fee_per_gas (int | str | None):
            Maximum priority fee per gas (EIP-1559).

        value (int | str | None):
            Amount of native token to transfer (in wei).

        data (str | bytes | None):
            Transaction calldata or contract creation bytecode.

        nonce (int | str | None):
            Transaction nonce.

        chain_id (int | str | None):
            Chain ID for replay protection.

        type_ (int | str | None):
            Transaction type (e.g. 0 for legacy, 2 for EIP-1559).

        access_list (list[Mapping[str, Any]] | None):
            Optional EIP-2930 access list, where each entry is a mapping with
            ``address`` and ``storageKeys``.

        strict (bool):
            Whether to enforce strict validation and type checking.

    Returns:
        dict[str, Any]:
            A normalized transaction object with canonical JSON-RPC field names.

    Raises:
        ValueError:
            If mutually incompatible fields are provided (e.g. mixing legacy and
            EIP-1559 fee parameters).

        TypeError:
            If inputs cannot be coerced into valid transaction field types.
    """
    tx: dict[str, Any] = {}

    if from_ is not None:
        tx["from"] = validation.normalize_address(from_, strict=strict)
    if to is not None:
        tx["to"] = validation.normalize_address(to, strict=strict)
    if gas is not None:
        tx["gas"] = validation.quantity(gas, strict=strict)
    if gas_price is not None:
        tx["gasPrice"] = validation.quantity(gas_price, strict=strict)
    if max_fee_per_gas is not None:
        tx["maxFeePerGas"] = validation.quantity(max_fee_per_gas, strict=strict)
    if max_priority_fee_per_gas is not None:
        tx["maxPriorityFeePerGas"] = validation.quantity(max_priority_fee_per_gas, strict=strict)
    if value is not None:
        tx["value"] = validation.quantity(value, strict=strict)
    if data is not None:
        tx["data"] = validation.data_hex(data, name="data", strict=strict, allow_empty=True)
    if nonce is not None:
        tx["nonce"] = validation.quantity(nonce, strict=strict)
    if chain_id is not None:
        tx["chainId"] = validation.quantity(chain_id, strict=strict)
    if type_ is not None:
        tx["type"] = validation.quantity(type_, strict=strict)
    if access_list is not None:
        tx["accessList"] = access_list

    validation.validate_tx_object(tx, strict=strict)
    return tx
