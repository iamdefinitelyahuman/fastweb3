# tests/unit/test_provider_freshness.py
from __future__ import annotations

import time
from typing import Any, Callable, Optional

import pytest

import fw3.provider.endpoint_selection as es_mod
import fw3.provider.execution as exec_mod
from fw3.errors import AllEndpointsFailed, NoEndpoints
from fw3.provider import Provider, RetryPolicy


class DummyPoolManager:
    """Minimal PoolManager stand-in to keep pooling enabled in Provider."""

    def best_urls(self, needed: int, await_first: bool, exclude: set) -> list[str]:
        return []


class FakeEndpoint:
    """
    Endpoint test double for Provider freshness/tip tests.

    - request_batch() pops scripted (tip, result)
    - can run an optional hook on each request_batch to simulate concurrency
    """

    def __init__(self, url: str, *args: Any, **kwargs: Any) -> None:
        self.url = url
        self.closed = False
        self.batch_queue: list[tuple[int, Any]] = []
        self.request_queue: list[Any] = []
        self.on_batch: Optional[Callable[[], None]] = None

    def queue_batch(self, tip: int, result: Any) -> None:
        self.batch_queue.append((int(tip), result))

    def queue_request(self, result: Any) -> None:
        self.request_queue.append(result)

    def request(self, method: str, params: Any, formatter: Any = None) -> Any:
        if not self.request_queue:
            raise AssertionError(f"FakeEndpoint({self.url}).request() called but queue empty")
        res = self.request_queue.pop(0)
        return formatter(res) if formatter is not None else res

    def request_batch(self, *calls: Any) -> tuple[Any, Any]:
        # Provider always sends [("eth_blockNumber"...), (user_call...)]
        if self.on_batch is not None:
            self.on_batch()

        if not self.batch_queue:
            raise AssertionError(f"FakeEndpoint({self.url}).request_batch() called but queue empty")
        tip, result = self.batch_queue.pop(0)

        # Provider passes to_int formatter for eth_blockNumber; we can return int directly.
        return tip, result

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def patch_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch the Endpoint class used inside Provider to our FakeEndpoint.
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)


def _fresh_latest(_resp: Any, required_tip: int, returned_tip: int) -> bool:
    return returned_tip >= required_tip


def test_tracks_best_tip_and_last_tip_updates(patch_endpoint: None) -> None:
    pm = DummyPoolManager()
    p = Provider(["http://a", "http://b"], pool_manager=pm, desired_pool_size=2)

    # RR first call hits "a"
    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)

    a.queue_batch(5, "A1")
    b.queue_batch(7, "B1")

    r1 = p.request("eth_test", [], route="pool")
    assert r1 == "A1"
    assert p._best_tip == 5
    assert p._state[a].last_tip == 5

    r2 = p.request("eth_test", [], route="pool")
    assert r2 == "B1"
    assert p._best_tip == 7
    assert p._state[b].last_tip == 7


def test_freshness_rejection_rotates_by_last_tip(patch_endpoint: None) -> None:
    pm = DummyPoolManager()
    p = Provider(["http://a", "http://b", "http://c"], pool_manager=pm, desired_pool_size=3)

    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    c = p._get_or_create_endpoint("http://c")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)
    assert isinstance(c, FakeEndpoint)

    # Seed last_tip + best_tip:
    # Call 1 (RR=a): tip=9
    # Call 2 (RR=b): tip=10 => best_tip becomes 10; last_tip set for a/b
    a.queue_batch(9, "seedA")
    b.queue_batch(10, "seedB")
    assert p.request("eth_seed", [], route="pool") == "seedA"
    assert p.request("eth_seed", [], route="pool") == "seedB"
    assert p._best_tip == 10
    assert p._state[a].last_tip == 9
    assert p._state[b].last_tip == 10

    # Force RR start back to "a" for the freshness-enforced call.
    p._rr = 0

    # Freshness call:
    # - "a" returns tip 9 (stale vs required 10) => rejected
    # - Then Provider should prefer highest last_tip => try "b" (last_tip 10) => accept
    a.queue_batch(9, "A_fresh_reject")
    b.queue_batch(10, "B_ok")
    c.queue_batch(8, "C_stale")

    before = time.time()
    res = p.request("eth_test", [], route="pool", freshness=_fresh_latest)
    assert res == "B_ok"

    # "a" should have been tip-demoted (cooldown in future).
    assert p._state[a].tip_cooldown_until > before


def test_concurrency_required_tip_snapshot_prevents_false_reject(patch_endpoint: None) -> None:
    pm = DummyPoolManager()
    p = Provider(["http://a", "http://b"], pool_manager=pm, desired_pool_size=2)

    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)

    # Pretend we've already observed tip 100.
    with p._lock:
        p._best_tip = 100

    # Ensure RR starts at "a".
    p._rr = 0

    # Simulate another concurrent call advancing best_tip to 101 while this attempt is in-flight.
    def advance_best_tip() -> None:
        with p._lock:
            p._best_tip = 101

    a.on_batch = advance_best_tip
    a.queue_batch(100, "A_result_at_100")

    # Freshness requires returned_tip >= required_tip.
    # required_tip should be snapshotted as 100 at attempt start, so this should ACCEPT,
    # even though best_tip becomes 101 during the request.
    res = p.request("eth_test", [], route="pool", freshness=_fresh_latest)
    assert res == "A_result_at_100"

    # Global best remains 101 (from the simulated concurrent advance).
    assert p._best_tip == 101


def test_no_endpoints_raises(patch_endpoint: None) -> None:
    # Pool manager present but no internal urls and best_urls returns [] => NoEndpoints.
    pm = DummyPoolManager()
    p = Provider([], pool_manager=pm, desired_pool_size=1)
    with pytest.raises(NoEndpoints):
        p.request("eth_test", [], route="pool")


def test_rpc_error_bubbles_immediately(
    patch_endpoint: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ensure Provider does not retry on RPCError (today's mission).
    pm = DummyPoolManager()
    p = Provider(["http://a", "http://b"], pool_manager=pm, desired_pool_size=2)

    a = p._get_or_create_endpoint("http://a")
    assert isinstance(a, FakeEndpoint)

    class DummyRPCError(Exception):
        pass

    # Patch provider_mod.RPCError to our dummy so Provider's except RPCError catches it.
    monkeypatch.setattr(exec_mod, "RPCError", DummyRPCError)

    def raise_rpc_error(*args: Any, **kwargs: Any) -> Any:
        raise DummyRPCError("rpc boom")

    # Patch only this endpoint's request_batch to raise.
    a.request_batch = raise_rpc_error  # type: ignore[method-assign]

    with pytest.raises(DummyRPCError, match="rpc boom"):
        p.request("eth_test", [], route="pool", freshness=_fresh_latest)


def test_freshness_unmet_eventually_errors(patch_endpoint: None) -> None:
    pm = DummyPoolManager()

    p = Provider(
        ["http://a", "http://b"],
        pool_manager=pm,
        desired_pool_size=2,
        retry_policy_pool=RetryPolicy(max_attempts=3, backoff_seconds=0.0),
    )
    p._FRESHNESS_WAIT_CAP_SECONDS = 0.0  # type: ignore[attr-defined]

    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)

    with p._lock:
        p._best_tip = 50

    # Only need one response per endpoint since we won't spin.
    a.queue_batch(49, "A")
    b.queue_batch(49, "B")

    with pytest.raises(AllEndpointsFailed):
        p.request("eth_test", [], route="pool", freshness=_fresh_latest)
