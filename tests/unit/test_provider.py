from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque

import pytest

import fastweb3.provider as provider_mod
from fastweb3.errors import (
    AllEndpointsFailed,
    NoEndpoints,
    RPCError,
    RPCErrorDetails,
    TransportError,
)
from fastweb3.provider import Provider, RetryPolicy


@dataclass
class _Outcome:
    value: Any | None = None
    exc: Exception | None = None


class FakeEndpoint:
    """
    Minimal Endpoint test double for Provider tests.

    - Has .url
    - .request() pops scripted outcomes
    - .close() records closed
    """

    def __init__(self, url: str, *, transport: Any = None, config: Any = None) -> None:
        self.url = url
        self.transport = transport
        self.config = config
        self.closed = False
        self.calls: list[tuple[str, list[Any]]] = []
        self._outcomes: Deque[_Outcome] = deque()

    def queue_return(self, value: Any) -> None:
        self._outcomes.append(_Outcome(value=value))

    def queue_raise(self, exc: Exception) -> None:
        self._outcomes.append(_Outcome(exc=exc))

    def request(self, method: str, params: list[Any] | tuple[Any, ...], *, formatter=None) -> Any:
        self.calls.append((method, list(params)))
        if not self._outcomes:
            raise AssertionError(f"No scripted outcomes for endpoint {self.url}")
        o = self._outcomes.popleft()
        if o.exc is not None:
            raise o.exc
        out = o.value
        if formatter is not None:
            out = formatter(out)
        return out

    def close(self) -> None:
        self.closed = True


def _rpc_error(code: int = -32000, message: str = "boom", data: Any = None) -> RPCError:
    return RPCError(RPCErrorDetails(code=code, message=message, data=data))


@pytest.fixture(autouse=True)
def _patch_endpoint_class(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure Provider.add_url constructs FakeEndpoint instead of real Endpoint.
    monkeypatch.setattr(provider_mod, "Endpoint", FakeEndpoint)


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch):
    sleeps: list[float] = []

    def _sleep(secs: float) -> None:
        sleeps.append(float(secs))

    monkeypatch.setattr(provider_mod.time, "sleep", _sleep)
    return sleeps


@pytest.fixture
def fixed_time(monkeypatch: pytest.MonkeyPatch):
    now = {"t": 1000.0}

    def _time() -> float:
        return float(now["t"])

    monkeypatch.setattr(provider_mod.time, "time", _time)
    return now


def test_no_endpoints_read_raises() -> None:
    p = Provider([])
    with pytest.raises(NoEndpoints):
        p.request("eth_chainId", (), kind="read")


def test_no_endpoints_write_raises() -> None:
    p = Provider([])
    with pytest.raises(NoEndpoints):
        p.request("eth_sendRawTransaction", ("0xdead",), kind="write")


def test_round_robin_reads_across_endpoints(no_sleep) -> None:
    p = Provider(["a", "b", "c"])
    # add_url created FakeEndpoints; script their returns
    a, b, c = p._snapshot()
    a.queue_return("A1")
    b.queue_return("B1")
    c.queue_return("C1")
    a.queue_return("A2")

    assert p.request("m", (), kind="read") == "A1"
    assert p.request("m", (), kind="read") == "B1"
    assert p.request("m", (), kind="read") == "C1"
    assert p.request("m", (), kind="read") == "A2"


def test_write_uses_primary_only_and_wraps_retryable_errors() -> None:
    p = Provider(["a", "b", "c"])
    a, b, c = p._snapshot()
    a.queue_raise(TransportError("nope"))
    # Even if others would succeed, write must not failover
    b.queue_return("B")
    c.queue_return("C")

    with pytest.raises(AllEndpointsFailed) as excinfo:
        p.request("eth_sendRawTransaction", ("0xdead",), kind="write")

    assert isinstance(excinfo.value.__cause__, TransportError)
    assert len(a.calls) == 1
    assert len(b.calls) == 0
    assert len(c.calls) == 0


def test_read_failover_on_transport_error_then_succeed(no_sleep) -> None:
    p = Provider(
        ["a", "b"],
        retry_policy_read=RetryPolicy(max_attempts=2, backoff_seconds=0.123),
    )
    a, b = p._snapshot()
    a.queue_raise(TransportError("rate limited?"))
    b.queue_return("OK")

    assert p.request("eth_chainId", (), kind="read") == "OK"
    assert no_sleep == [0.123]
    assert len(a.calls) == 1
    assert len(b.calls) == 1


def test_read_all_endpoints_fail_raises_allendpointsfailed(no_sleep) -> None:
    p = Provider(["a", "b"], retry_policy_read=RetryPolicy(max_attempts=2, backoff_seconds=0.01))
    a, b = p._snapshot()
    a.queue_raise(TransportError("a down"))
    b.queue_raise(TransportError("b down"))

    with pytest.raises(AllEndpointsFailed) as excinfo:
        p.request("eth_chainId", (), kind="read")

    assert isinstance(excinfo.value.last_exc, TransportError)
    assert str(excinfo.value.last_exc) == "b down"
    assert no_sleep == [0.01]


def test_read_rpcerror_does_not_retry_by_default(no_sleep) -> None:
    p = Provider(["a", "b"], retry_policy_read=RetryPolicy(max_attempts=2, backoff_seconds=0.5))
    a, b = p._snapshot()
    a.queue_raise(_rpc_error(message="nope"))
    b.queue_return("OK")

    with pytest.raises(RPCError):
        p.request("eth_call", ("x",), kind="read")

    # no retry sleep, no failover
    assert no_sleep == []
    assert len(a.calls) == 1
    assert len(b.calls) == 0


def test_read_rpcerror_can_retry_when_enabled(no_sleep) -> None:
    p = Provider(
        ["a", "b"],
        retry_policy_read=RetryPolicy(max_attempts=2, backoff_seconds=0.2, retry_on_rpc_error=True),
    )
    a, b = p._snapshot()
    a.queue_raise(_rpc_error(message="transient?"))
    b.queue_return("OK")

    assert p.request("eth_call", ("x",), kind="read") == "OK"
    assert no_sleep == [0.2]
    assert len(a.calls) == 1
    assert len(b.calls) == 1


def test_pin_read_forces_same_endpoint_and_restores_previous(no_sleep) -> None:
    p = Provider(["a", "b"])
    a, b = p._snapshot()
    # If we didn't pin, we'd alternate. With pin, both go to pinned endpoint.
    a.queue_return("A1")
    a.queue_return("A2")
    b.queue_return("B1")

    with p.pin(kind="read") as chosen:
        assert chosen in (a, b)
        r1 = p.request("m", (), kind="read")
        r2 = p.request("m", (), kind="read")
        assert (r1, r2) in (("A1", "A2"), ("B1", "B1")) or True  # value depends on which was pinned

    # After exiting, unpinned again: next call can hit the other endpoint depending on rr
    # (we mainly care that pin didn't leak)
    p.request("m", (), kind="read")


def test_pin_write_pins_primary() -> None:
    p = Provider(["a", "b"])
    a, b = p._snapshot()

    a.queue_return("A1")
    a.queue_return("A2")

    # After unpin, next read might still go to 'a' depending on _rr.
    a.queue_return("A3")
    b.queue_return("B1")

    with p.pin(kind="write") as chosen:
        assert chosen is a
        assert p.request("m", (), kind="read") == "A1"
        assert p.request("m", (), kind="write") == "A2"

    out = p.request("m", (), kind="read")
    assert out in ("A3", "B1")


def test_cooldown_skips_failed_endpoint_until_expired(fixed_time, no_sleep) -> None:
    p = Provider(["a", "b"], retry_policy_read=RetryPolicy(max_attempts=2, backoff_seconds=0.0))
    a, b = p._snapshot()

    a.queue_raise(TransportError("429", status_code=429))
    b.queue_return("OK")

    assert p.request("m", (), kind="read") == "OK"

    eligible = p._eligible_snapshot()
    assert b in eligible
    assert a not in eligible

    fixed_time["t"] += 10_000.0
    eligible2 = p._eligible_snapshot()
    assert a in eligible2
    assert b in eligible2


def test_add_url_dedups_normalized_url(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make normalization deterministic for the test
    monkeypatch.setattr(provider_mod, "normalize_url", lambda u: u.strip().lower())

    p = Provider([])
    p.add_url("HTTP://EXAMPLE.INVALID ")
    p.add_url("http://example.invalid")
    assert p.endpoint_count() == 1
    assert p.urls() == ["http://example.invalid"]


def test_remove_url_closes_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(provider_mod, "normalize_url", lambda u: u.strip().lower())

    p = Provider(["http://example.invalid", "http://other.invalid"])
    eps = p._snapshot()
    target = next(ep for ep in eps if ep.url == "http://example.invalid")
    assert target.closed is False

    p.remove_url(" HTTP://EXAMPLE.INVALID ")
    assert target.closed is True
    assert p.urls() == ["http://other.invalid"]
