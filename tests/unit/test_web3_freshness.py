# tests/unit/test_web3_freshness.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import pytest

import fastweb3.web3 as web3_mod
from fastweb3.web3 import Web3


class _ImmediateHandle:
    def __init__(self) -> None:
        self._value: Any = None

    def set_value(self, v: Any) -> None:
        self._value = v

    @property
    def value(self) -> Any:
        return self._value


def _immediate_deferred_response(bg_func, format_func=None, ref_func=None):
    """
    Replace deferred_response with a synchronous executor for unit tests.

    - Runs bg_func immediately
    - Applies format_func if present
    - Returns the final value
    """
    h = _ImmediateHandle()
    bg_func(h)
    v = h.value
    if format_func is not None:
        return format_func(v)
    return v


@dataclass
class _RecordedCall:
    method: str
    params: list[Any]
    route: str
    freshness: Optional[Callable[[Any, int, int], bool]]


class FakeProvider:
    """
    Provider test double to capture `freshness` callables passed by Web3/Eth helpers.
    """

    def __init__(self) -> None:
        self.calls: list[_RecordedCall] = []

    def request(
        self, method: str, params: list[Any], *, route: str, formatter=None, freshness=None
    ) -> Any:
        self.calls.append(
            _RecordedCall(method=method, params=list(params), route=route, freshness=freshness)
        )
        # Return something "raw" that makes formatter tests easy.
        # When formatter=to_int, Web3.make_request applies as format_func (not provider.formatter)
        return "0x2a"  # 42

    def close(self) -> None:
        return None

    # used by Web3.pin()
    def pin(self, *, route: str = "pool"):
        raise AssertionError("pin() not expected in these tests")


@pytest.fixture
def patch_deferred(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(web3_mod, "deferred_response", _immediate_deferred_response)


def test_fresh_latest_callable_behavior() -> None:
    assert web3_mod._fresh_latest("x", 10, 10) is True
    assert web3_mod._fresh_latest("x", 10, 11) is True
    assert web3_mod._fresh_latest("x", 10, 9) is False


def test_fresh_negative_requires_latest_callable_behavior() -> None:
    # Any positive result is acceptable even if endpoint tip is behind.
    assert web3_mod._fresh_negative_requires_latest({"ok": True}, 10, 0) is True
    assert web3_mod._fresh_negative_requires_latest([], 10, 9) is True
    assert web3_mod._fresh_negative_requires_latest("0xabc", 10, 1) is True

    # Negative result requires tip >= required_tip.
    assert web3_mod._fresh_negative_requires_latest(None, 10, 9) is False
    assert web3_mod._fresh_negative_requires_latest(None, 10, 10) is True
    assert web3_mod._fresh_negative_requires_latest(None, 10, 11) is True


def test_web3_make_request_passes_freshness_through(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    out = w3.make_request("eth_getBalance", ["0xabc", "latest"], freshness=web3_mod._fresh_latest)
    assert out == "0x2a"
    assert fp.calls[-1].freshness is web3_mod._fresh_latest


def test_eth_get_balance_latest_uses_fresh_latest(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    # This should attach freshness since block is latest-like.
    out = w3.eth.get_balance("0x" + "11" * 20, "latest")
    assert out == 42  # formatter=to_int applied by Web3.make_request

    call = fp.calls[-1]
    assert call.method == "eth_getBalance"
    assert call.freshness is web3_mod._fresh_latest


def test_eth_get_balance_historic_block_uses_no_freshness(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.get_balance("0x" + "11" * 20, 123)
    call = fp.calls[-1]
    assert call.method == "eth_getBalance"
    assert call.freshness is None


def test_eth_call_pending_uses_fresh_latest(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.call(to="0x" + "22" * 20, data="0x", block="pending")
    call = fp.calls[-1]
    assert call.method == "eth_call"
    assert call.freshness is web3_mod._fresh_latest


def test_eth_estimate_gas_block_none_uses_fresh_latest(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.estimate_gas(to="0x" + "22" * 20, data="0x")
    call = fp.calls[-1]
    assert call.method == "eth_estimateGas"
    assert call.freshness is web3_mod._fresh_latest


def test_eth_get_transaction_by_hash_uses_negative_requires_latest(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.get_transaction_by_hash("0x" + "33" * 32)
    call = fp.calls[-1]
    assert call.method == "eth_getTransactionByHash"
    assert call.freshness is web3_mod._fresh_negative_requires_latest


def test_eth_get_transaction_receipt_uses_negative_requires_latest(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.get_transaction_receipt("0x" + "44" * 32)
    call = fp.calls[-1]
    assert call.method == "eth_getTransactionReceipt"
    assert call.freshness is web3_mod._fresh_negative_requires_latest


def test_eth_get_logs_latest_like_to_block_uses_fresh_latest(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.get_logs()  # to_block omitted => latest-like
    call = fp.calls[-1]
    assert call.method == "eth_getLogs"
    assert call.freshness is web3_mod._fresh_latest


def test_eth_get_logs_historic_to_block_uses_no_freshness(patch_deferred: None) -> None:
    fp = FakeProvider()
    w3 = Web3(provider=fp)

    _ = w3.eth.get_logs(from_block=100, to_block=200)
    call = fp.calls[-1]
    assert call.method == "eth_getLogs"
    assert call.freshness is None
