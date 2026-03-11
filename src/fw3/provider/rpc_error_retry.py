"""Heuristics for retrying safe JSON-RPC calls when RPC errors occur."""

from __future__ import annotations

import re
from typing import Sequence

from .types import _BatchCall, _RPCErrorObservation, _RPCErrorRetryDecision

RPC_ERROR_SAFE_READ_METHODS = {
    "eth_call",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getTransactionCount",
    "eth_getProof",
    "eth_blockNumber",
    "eth_getBlockByNumber",
    "eth_getBlockTransactionCountByNumber",
    "eth_getUncleCountByBlockNumber",
    "eth_chainId",
    "net_version",
}

RPC_ERROR_DETERMINISTIC_NEEDLES = (
    "execution reverted",
    "revert",
    "invalid argument",
    "invalid params",
    "invalid parameter",
    "method not found",
    "does not exist/is not available",
    "unsupported method",
    "insufficient funds",
    "nonce too low",
    "nonce too high",
    "replacement transaction underpriced",
    "transaction underpriced",
    "already known",
    "known transaction",
    "intrinsic gas too low",
    "max fee per gas less than block base fee",
    "fee cap less than block base fee",
    "gas required exceeds allowance",
    "exceeds block gas limit",
    "sender doesn't have enough funds",
    "sender doesnt have enough funds",
    "invalid sender",
)

RPC_ERROR_RATE_LIMIT_NEEDLES = (
    "rate limit",
    "too many requests",
    "429",
    "quota",
    "throttle",
    "throttled",
)

RPC_ERROR_NODE_HEALTH_NEEDLES = (
    "missing trie node",
    "header not found",
    "unknown block",
    "block not found",
    "state unavailable",
    "state is not available",
    "state not available",
    "world state unavailable",
    "missing state",
    "pruned",
    "historical state unavailable",
)

RPC_ERROR_TRANSIENT_NEEDLES = (
    "internal error",
    "server error",
    "upstream",
    "timeout",
    "timed out",
    "gateway timeout",
    "bad gateway",
    "service unavailable",
    "temporarily unavailable",
    "try again later",
    "busy",
    "overloaded",
    "unavailable",
    "connection reset",
    "econnreset",
    "socket hang up",
    "500",
    "502",
    "503",
    "504",
)

BLOCK_ARG_SECOND_POSITION_METHODS = {
    "eth_call",
    "eth_getBalance",
    "eth_getStorageAt",
    "eth_getCode",
    "eth_getTransactionCount",
    "eth_getProof",
}

BLOCK_ARG_FIRST_POSITION_METHODS = {
    "eth_getBlockByNumber",
    "eth_getBlockTransactionCountByNumber",
    "eth_getUncleCountByBlockNumber",
}


def _normalize_rpc_error_message(message: str) -> str:
    """Normalize an RPC error message for rough equality comparisons."""
    msg = (message or "").lower()
    msg = re.sub(r"0x[0-9a-f]+", "0x", msg)
    msg = re.sub(r"\b\d+\b", "#", msg)
    msg = re.sub(r"\s+", " ", msg).strip()
    return msg


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(n in text for n in needles)


def _pin_calls_to_tip(calls: list[_BatchCall], tip: int) -> list[_BatchCall]:
    """Return copies of calls pinned to a specific block tip where applicable."""
    tip_hex = hex(tip)
    out: list[_BatchCall] = []

    for call in calls:
        params = list(call.params)

        if call.method in BLOCK_ARG_SECOND_POSITION_METHODS:
            if len(params) < 2:
                params.append(tip_hex)
            elif params[1] == "latest":
                params[1] = tip_hex

        elif call.method in BLOCK_ARG_FIRST_POSITION_METHODS:
            if params and params[0] == "latest":
                params[0] = tip_hex

        out.append(
            _BatchCall(
                method=call.method,
                params=tuple(params) if isinstance(call.params, tuple) else params,
                formatter=call.formatter,
                freshness=call.freshness,
            )
        )

    return out


def _decide_rpc_error_retry(
    *, call: _BatchCall, history: Sequence[_RPCErrorObservation]
) -> _RPCErrorRetryDecision:
    """Return a retry decision for a single RPCError response.

    `history` contains all observations so far for this call index, including the
    most recent one (i.e. the current RPCError).
    """
    if not history:
        return _RPCErrorRetryDecision()

    current = history[-1]
    code = current.code
    norm = current.normalized_message

    if call.method not in RPC_ERROR_SAFE_READ_METHODS:
        return _RPCErrorRetryDecision()

    if code in (-32601, -32602) or _contains_any(norm, RPC_ERROR_DETERMINISTIC_NEEDLES):
        return _RPCErrorRetryDecision()

    is_rate_limited = _contains_any(norm, RPC_ERROR_RATE_LIMIT_NEEDLES)
    is_node_health = _contains_any(norm, RPC_ERROR_NODE_HEALTH_NEEDLES)
    is_transient = _contains_any(norm, RPC_ERROR_TRANSIENT_NEEDLES) or code == -32603

    current_fingerprint = (code, norm)
    matching = [obs for obs in history if (obs.code, obs.normalized_message) == current_fingerprint]
    same_fingerprint_count = len({obs.endpoint_url for obs in matching})

    if is_rate_limited:
        if same_fingerprint_count >= 3:
            return _RPCErrorRetryDecision()
        return _RPCErrorRetryDecision(retry=True, demote_current_endpoint=False)

    if is_node_health:
        if same_fingerprint_count >= 3:
            return _RPCErrorRetryDecision()
        return _RPCErrorRetryDecision(retry=True, demote_current_endpoint=True)

    if is_transient:
        if same_fingerprint_count >= 3:
            return _RPCErrorRetryDecision()
        return _RPCErrorRetryDecision(retry=True, demote_current_endpoint=False)

    if same_fingerprint_count >= 3:
        return _RPCErrorRetryDecision()

    if len(history) <= 3:
        return _RPCErrorRetryDecision(retry=True, demote_current_endpoint=False)

    unique_fingerprints = {(obs.code, obs.normalized_message, obs.endpoint_url) for obs in history}
    if len(unique_fingerprints) == len(history):
        return _RPCErrorRetryDecision(retry=True, demote_current_endpoint=False)

    return _RPCErrorRetryDecision()
