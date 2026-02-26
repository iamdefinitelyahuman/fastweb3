# tests/unit/test_provider.py
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque

import pytest

import fastweb3.provider as provider_mod
import fastweb3.utils as utils_mod
from fastweb3.errors import (
    AllEndpointsFailed,
    NoEndpoints,
    NoPrimaryEndpoint,
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

    def __init__(self, url: str, *args: Any, **kwargs: Any) -> None:
        self.url = url
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


class FakePoolManager:
    """
    Minimal PoolManager test double.

    best_urls(n, await_first) returns up to n URLs from an internal list.
    Records calls as tuples: (n, await_first).
    """

    def __init__(self, urls: list[str]) -> None:
        self._urls = list(urls)
        self.calls: list[tuple[int, bool]] = []

    def best_urls(self, n: int, await_first: bool) -> list[str]:
        self.calls.append((int(n), bool(await_first)))
        if n <= 0:
            return []
        return list(self._urls[:n])


def _rpc_error(code: int = -32000, message: str = "boom", data: Any = None) -> RPCError:
    return RPCError(RPCErrorDetails(code=code, message=message, data=data))


@pytest.fixture(autouse=True)
def _patch_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure Provider constructs FakeEndpoint.
    monkeypatch.setattr(provider_mod, "Endpoint", FakeEndpoint)


@pytest.fixture(autouse=True)
def _patch_normalize(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure Provider uses the real normalize_url by default (includes env expansion).
    monkeypatch.setattr(provider_mod, "normalize_url", utils_mod.normalize_url)


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


def _ep(p: Provider, url: str) -> FakeEndpoint:
    # Helper: grab cached endpoint by URL.
    nu = provider_mod.normalize_url(url)
    return p._eps_by_url[nu]  # type: ignore[attr-defined]


def test_no_endpoints_pool_route_raises() -> None:
    p = Provider([])
    with pytest.raises(NoEndpoints):
        p.request("eth_chainId", (), route="pool")


def test_primary_route_requires_primary() -> None:
    p = Provider(["a"])
    with pytest.raises(NoPrimaryEndpoint):
        p.request("eth_chainId", (), route="primary")


def test_primary_route_hits_primary_only_and_wraps_retryable_errors() -> None:
    p = Provider(["a", "b", "c"])
    p.set_primary("a")

    a = _ep(p, "a")
    b = _ep(p, "b")
    c = _ep(p, "c")

    a.queue_raise(TransportError("nope"))
    b.queue_return("B")
    c.queue_return("C")

    with pytest.raises(AllEndpointsFailed) as excinfo:
        p.request("eth_sendRawTransaction", ("0xdead",), route="primary")

    assert isinstance(excinfo.value.__cause__, TransportError)
    assert len(a.calls) == 1
    assert len(b.calls) == 0
    assert len(c.calls) == 0


def test_pool_round_robin_across_internal_endpoints(no_sleep) -> None:
    p = Provider(["a", "b", "c"])

    a = _ep(p, "a")
    b = _ep(p, "b")
    c = _ep(p, "c")

    a.queue_return("A1")
    b.queue_return("B1")
    c.queue_return("C1")
    a.queue_return("A2")

    assert p.request("m", (), route="pool") == "A1"
    assert p.request("m", (), route="pool") == "B1"
    assert p.request("m", (), route="pool") == "C1"
    assert p.request("m", (), route="pool") == "A2"


def test_pool_candidates_merge_internal_then_manager_fill_and_dedup() -> None:
    pm = FakePoolManager(["b", "d", "e"])
    p = Provider(["a", "b"], pool_manager=pm, desired_pool_size=4)

    a = _ep(p, "a")
    b = _ep(p, "b")
    d = p._get_or_create_endpoint("d")  # type: ignore[attr-defined]

    a.queue_return("A")
    b.queue_return("B")
    d.queue_return("D")

    # First pool call should ask manager for 2 (need 4 total, have 2 internal)
    assert p.request("m", (), route="pool") == "A"
    assert pm.calls == [(2, False)]  # await_first False because internal exists

    out = p.request("m", (), route="pool")
    assert out in ("B", "D")


def test_pool_failover_on_transport_error_then_succeed(no_sleep) -> None:
    p = Provider(
        ["a", "b"],
        retry_policy_pool=RetryPolicy(max_attempts=2, backoff_seconds=0.123),
    )
    a = _ep(p, "a")
    b = _ep(p, "b")

    a.queue_raise(TransportError("rate limited?"))
    b.queue_return("OK")

    assert p.request("eth_chainId", (), route="pool") == "OK"
    assert no_sleep == [0.123]
    assert len(a.calls) == 1
    assert len(b.calls) == 1


def test_pool_all_endpoints_fail_raises_allendpointsfailed(no_sleep) -> None:
    p = Provider(["a", "b"], retry_policy_pool=RetryPolicy(max_attempts=2, backoff_seconds=0.01))
    a = _ep(p, "a")
    b = _ep(p, "b")

    a.queue_raise(TransportError("a down"))
    b.queue_raise(TransportError("b down"))

    with pytest.raises(AllEndpointsFailed) as excinfo:
        p.request("eth_chainId", (), route="pool")

    assert isinstance(excinfo.value.last_exc, TransportError)
    assert str(excinfo.value.last_exc) == "b down"
    assert no_sleep == [0.01]


def test_pool_rpcerror_does_not_retry_by_default(no_sleep) -> None:
    p = Provider(["a", "b"], retry_policy_pool=RetryPolicy(max_attempts=2, backoff_seconds=0.5))
    a = _ep(p, "a")
    b = _ep(p, "b")

    a.queue_raise(_rpc_error(message="nope"))
    b.queue_return("OK")

    with pytest.raises(RPCError):
        p.request("eth_call", ("x",), route="pool")

    assert no_sleep == []
    assert len(a.calls) == 1
    assert len(b.calls) == 0


def test_pool_rpcerror_can_retry_when_enabled(no_sleep) -> None:
    p = Provider(
        ["a", "b"],
        retry_policy_pool=RetryPolicy(max_attempts=2, backoff_seconds=0.2, retry_on_rpc_error=True),
    )
    a = _ep(p, "a")
    b = _ep(p, "b")

    a.queue_raise(_rpc_error(message="transient?"))
    b.queue_return("OK")

    assert p.request("eth_call", ("x",), route="pool") == "OK"
    assert no_sleep == [0.2]
    assert len(a.calls) == 1
    assert len(b.calls) == 1


def test_pin_pool_forces_same_endpoint_and_restores_previous() -> None:
    p = Provider(["a", "b"])
    a = _ep(p, "a")
    b = _ep(p, "b")

    # Script enough outcomes for whichever endpoint gets pinned
    a.queue_return("A1")
    a.queue_return("A2")
    b.queue_return("B1")
    b.queue_return("B2")

    with p.pin(route="pool") as chosen:
        assert chosen in (a, b)
        r1 = p.request("m", (), route="pool")
        r2 = p.request("m", (), route="pool")
        # Both calls should go to the pinned endpoint (so same letter)
        assert r1[0] == r2[0]

    # pin should not leak
    assert getattr(p._tls, "pinned", None) is None

    # After exiting, unpinned again: just ensure it works (RR may hit either endpoint).
    a.queue_return("A3")
    b.queue_return("B3")
    _ = p.request("m", (), route="pool")


def test_pin_primary_pins_primary_and_overrides_route_while_pinned() -> None:
    p = Provider(["a", "b"])
    p.set_primary("a")

    a = _ep(p, "a")
    b = _ep(p, "b")

    a.queue_return("A1")
    a.queue_return("A2")
    b.queue_return("B1")

    with p.pin(route="primary") as chosen:
        assert chosen is a
        assert p.request("m", (), route="pool") == "A1"
        assert p.request("m", (), route="primary") == "A2"

    # After unpin, pool routing can hit either; ensure both have outcomes.
    a.queue_return("A3")
    b.queue_return("B2")
    out = p.request("m", (), route="pool")
    assert out in ("A3", "B1", "B2")


def test_cooldown_skips_failed_endpoint_until_expired(fixed_time, no_sleep) -> None:
    p = Provider(["a", "b"], retry_policy_pool=RetryPolicy(max_attempts=2, backoff_seconds=0.0))
    a = _ep(p, "a")
    b = _ep(p, "b")

    a.queue_raise(TransportError("429", status_code=429))
    b.queue_return("OK")

    assert p.request("m", (), route="pool") == "OK"

    candidates = p._pool_candidates()  # type: ignore[attr-defined]
    eligible = p._eligible_endpoints(candidates)  # type: ignore[attr-defined]
    assert b in eligible
    assert a not in eligible

    fixed_time["t"] += 10_000.0
    eligible2 = p._eligible_endpoints(candidates)  # type: ignore[attr-defined]
    assert a in eligible2
    assert b in eligible2


def test_add_url_dedups_normalized_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(provider_mod, "normalize_url", lambda u: u.strip().lower())

    p = Provider([])
    p.add_url("HTTP://EXAMPLE.INVALID ")
    p.add_url("http://example.invalid")
    assert p.endpoint_count() == 1
    assert p.urls() == ["http://example.invalid"]


def test_remove_url_removes_from_internal_pool_but_does_not_close_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_mod, "normalize_url", lambda u: u.strip().lower())

    p = Provider(["http://example.invalid", "http://other.invalid"])
    target = p._eps_by_url["http://example.invalid"]  # type: ignore[attr-defined]
    assert target.closed is False

    p.remove_url(" HTTP://EXAMPLE.INVALID ")
    assert target.closed is False  # remove_url does not close cached endpoints
    assert p.urls() == ["http://other.invalid"]

    p.close()
    assert target.closed is True


def test_poolmanager_await_first_false_when_internal_present() -> None:
    pm = FakePoolManager(["m1", "m2"])
    p = Provider(["a"], pool_manager=pm, desired_pool_size=2)

    # Drive one pool request; should consult pool manager to fill 1 slot
    a = _ep(p, "a")
    m1 = p._get_or_create_endpoint("m1")  # type: ignore[attr-defined]

    a.queue_return("A")
    m1.queue_return("M1")

    out = p.request("m", (), route="pool")
    assert out in ("A", "M1")
    assert pm.calls == [(1, False)]  # internal exists => never await


def test_poolmanager_await_first_false_when_primary_present_and_internal_empty() -> None:
    pm = FakePoolManager(["m1"])
    p = Provider([], pool_manager=pm, desired_pool_size=1)
    p.set_primary("p1")

    # Even though internal is empty, primary exists => await_first False
    p1 = _ep(p, "p1")
    p1.queue_return("P1")

    # Manager might be asked (depending on desired_pool_size) but must be await_first False
    # In this configuration: needed = 1 (internal empty), so manager is called.
    # If manager returns empty, Provider would fall back to primary; ours returns ["m1"].
    m1 = p._get_or_create_endpoint("m1")  # type: ignore[attr-defined]
    m1.queue_return("M1")

    out = p.request("m", (), route="pool")
    assert out in ("M1",)  # pool candidates non-empty, so it can pick m1
    assert pm.calls == [(1, False)]


def test_poolmanager_await_first_true_when_no_internal_and_no_primary() -> None:
    pm = FakePoolManager(["m1"])
    p = Provider([], pool_manager=pm, desired_pool_size=1)

    m1 = p._get_or_create_endpoint("m1")  # type: ignore[attr-defined]
    m1.queue_return("M1")

    assert p.request("m", (), route="pool") == "M1"
    assert pm.calls == [(1, True)]


def test_provider_add_url_expands_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RPC_HOST", "node.local")
    p = Provider([])
    p.add_url("https://$RPC_HOST/rpc")
    assert p.urls() == ["https://node.local/rpc"]


def test_provider_set_primary_expands_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RPC_HOST", "node.local")
    p = Provider([])
    p.set_primary("https://$RPC_HOST/rpc/")
    assert p.primary_url() == "https://node.local/rpc"


def test_provider_add_url_missing_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RPC_MISSING", raising=False)
    p = Provider([])
    with pytest.raises(ValueError, match="RPC_MISSING.*not set|not.*set"):
        p.add_url("https://example.com/$RPC_MISSING")
