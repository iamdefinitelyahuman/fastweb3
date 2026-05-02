from __future__ import annotations

import pytest

from fw3.provider.rpc_error_retry import (
    _decide_rpc_error_retry,
    _normalize_rpc_error_message,
)
from fw3.provider.types import _BatchCall, _RPCErrorObservation


def _call(method: str = "eth_call") -> _BatchCall:
    return _BatchCall(method=method, params=[])


def _obs(url: str, code: int | None, message: str) -> _RPCErrorObservation:
    return _RPCErrorObservation(
        endpoint_url=url,
        code=code,
        message=message,
        normalized_message=_normalize_rpc_error_message(message),
    )


def test_decide_retry_empty_history_accepts_immediately() -> None:
    decision = _decide_rpc_error_retry(call=_call(), history=[])
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


def test_decide_retry_non_safe_method_never_retries() -> None:
    decision = _decide_rpc_error_retry(
        call=_call("eth_sendRawTransaction"),
        history=[_obs("a", -32603, "internal error")],
    )
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


@pytest.mark.parametrize(
    ("code", "message"),
    [
        (-32602, "invalid params"),
        (-32000, "execution reverted: nope"),
        (-32000, "nonce too low"),
    ],
)
def test_decide_retry_deterministic_errors_never_retry(code: int, message: str) -> None:
    decision = _decide_rpc_error_retry(
        call=_call(),
        history=[_obs("a", code, message)],
    )
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


@pytest.mark.parametrize(
    "message",
    [
        "method not found",
        "the method eth_call does not exist/is not available",
        "rpc method is not whitelisted",
    ],
)
def test_decide_retry_method_unsupported_errors_retry_with_demote_and_unsupported_method(
    message: str,
) -> None:
    decision = _decide_rpc_error_retry(
        call=_call("eth_call"),
        history=[_obs("a", -32601, message)],
    )
    assert decision.retry is True
    assert decision.demote_current_endpoint is True
    assert decision.unsupported_method is True


def test_decide_retry_method_unsupported_error_does_not_retry_unsafe_method() -> None:
    decision = _decide_rpc_error_retry(
        call=_call("eth_sendRawTransaction"),
        history=[_obs("a", -32601, "rpc method is not whitelisted")],
    )
    assert decision.retry is False
    assert decision.demote_current_endpoint is False
    assert decision.unsupported_method is False


def test_decide_retry_rate_limit_retries_without_demote_then_stops_after_three_matching_endpoints():
    history = [
        _obs("a", -32000, "429 Too Many Requests"),
        _obs("b", -32000, "429 too many requests"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is True
    assert decision.demote_current_endpoint is False

    history.append(_obs("c", -32000, "429 TOO MANY REQUESTS"))
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


def test_decide_retry_node_health_retries_with_demote_then_stops_after_three_matching_endpoints():
    history = [
        _obs("a", -32000, "missing trie node 0xabc"),
        _obs("b", -32000, "missing trie node 0xdef"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is True
    assert decision.demote_current_endpoint is True

    history.append(_obs("c", -32000, "missing trie node 0x123"))
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


def test_transient_internal_error_retries_without_demote_stops_after_three_matching_endpoints():
    history = [
        _obs("a", -32603, "Internal error"),
        _obs("b", -32603, "internal error"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is True
    assert decision.demote_current_endpoint is False

    history.append(_obs("c", -32603, "INTERNAL ERROR"))
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


def test_decide_retry_ambiguous_first_three_attempts_retry() -> None:
    history = [
        _obs("a", -32000, "strange thing happened"),
        _obs("b", -32000, "different weird failure"),
        _obs("c", -32000, "unexpected provider hiccup"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is True
    assert decision.demote_current_endpoint is False


def test_decide_retry_ambiguous_same_normalized_error_stops_after_three_distinct_endpoints() -> (
    None
):
    history = [
        _obs("a", -32000, "weird failure on block 123"),
        _obs("b", -32000, "weird failure on block 456"),
        _obs("c", -32000, "weird failure on block 789"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


def test_decide_retry_ambiguous_then_clear_deterministic_failure_stops_immediately() -> None:
    history = [
        _obs("a", -32603, "internal error"),
        _obs("b", -32000, "execution reverted: failed"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is False
    assert decision.demote_current_endpoint is False


def test_decide_retry_more_than_three_distinct_ambiguous_errors_can_still_retry() -> None:
    history = [
        _obs("a", -32000, "oddity one"),
        _obs("b", -32001, "oddity two"),
        _obs("c", -32002, "oddity three"),
        _obs("d", -32003, "oddity four"),
    ]
    decision = _decide_rpc_error_retry(call=_call(), history=history)
    assert decision.retry is True
    assert decision.demote_current_endpoint is False


def test_normalize_rpc_error_message_collapses_hex_numbers_and_whitespace() -> None:
    left = _normalize_rpc_error_message("  Missing trie node 0xABC at block 123   ")
    right = _normalize_rpc_error_message("missing trie node 0xdef at block 999")
    assert left == right
