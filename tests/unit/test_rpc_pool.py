# tests/unit/test_rpc_pool.py
from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest

import fastweb3.rpc_pool as rpc_pool

_REAL_THREAD = threading.Thread


class DummyResp:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeTransport:
    """
    Default fake transport for probe tests.

    Set FakeTransport.behavior[url] to either:
      - a response object to return from send(payload)
      - a callable(payload)->response
      - or omit to raise
    """

    behavior: dict[str, object] = {}

    def __init__(self, url: str) -> None:
        self.url = url
        self.closed = False

    def send(self, payload):
        fn = self.behavior.get(self.url)
        if fn is None:
            raise RuntimeError(f"no behavior for {self.url}")
        if callable(fn):
            return fn(payload)
        return fn

    def close(self) -> None:
        self.closed = True


def _batch_ok(chain_id: int, head_hex: str = "0x10"):
    return [
        {"jsonrpc": "2.0", "id": 1, "result": hex(chain_id)},
        {"jsonrpc": "2.0", "id": 2, "result": head_hex},
    ]


def pr(url: str, rtt: float, head: int) -> rpc_pool.ProbeResult:
    return rpc_pool.ProbeResult(url=url, rtt_ms=rtt, head=head)


@pytest.fixture(autouse=True)
def reset_shared_pool_registry():
    """Ensure each test starts with a clean global registry + no running scheduler thread."""
    # Best-effort stop scheduler if running
    try:
        t = getattr(rpc_pool, "_sched_thread", None)
        stop = getattr(rpc_pool, "_sched_stop", None)
        if stop is not None:
            stop.set()
        if t is not None and getattr(t, "is_alive", lambda: False)():
            if threading.current_thread() is not t:
                t.join(timeout=0.2)
    except Exception:
        pass

    with rpc_pool._pool_lock:
        rpc_pool._pool_by_chain.clear()
        rpc_pool._pool_refcount.clear()
        rpc_pool._sched_thread = None
        rpc_pool._sched_stop.clear()

    yield

    # Clean again (in case tests left anything behind)
    try:
        t = getattr(rpc_pool, "_sched_thread", None)
        stop = getattr(rpc_pool, "_sched_stop", None)
        if stop is not None:
            stop.set()
        if t is not None and getattr(t, "is_alive", lambda: False)():
            if threading.current_thread() is not t:
                t.join(timeout=0.2)
    except Exception:
        pass

    with rpc_pool._pool_lock:
        rpc_pool._pool_by_chain.clear()
        rpc_pool._pool_refcount.clear()
        rpc_pool._sched_thread = None
        rpc_pool._sched_stop.clear()


@pytest.fixture
def fake_make_transport(monkeypatch: pytest.MonkeyPatch):
    """Make rpc_pool use FakeTransport regardless of scheme, and ignore config kwargs."""

    def _fake_make_transport(url: str, **kwargs: Any) -> FakeTransport:
        return FakeTransport(url)

    monkeypatch.setattr(rpc_pool, "make_transport", _fake_make_transport)


def test_hex_to_int_ok():
    assert rpc_pool._hex_to_int("0x0") == 0
    assert rpc_pool._hex_to_int("0x10") == 16
    assert rpc_pool._hex_to_int("0xFF") == 255


@pytest.mark.parametrize("bad", [None, 123, "10", "0X10", "0x", "potato"])
def test_hex_to_int_bad_raises(bad):
    with pytest.raises(ValueError):
        rpc_pool._hex_to_int(bad)


def test_is_probeable(monkeypatch: pytest.MonkeyPatch):
    # default: assume no WSS support => ws/wss not probeable
    monkeypatch.setattr(rpc_pool, "_has_wss_support", lambda: False)

    assert rpc_pool._is_probeable("http://x")
    assert rpc_pool._is_probeable("https://x")

    assert not rpc_pool._is_probeable("ws://x")
    assert not rpc_pool._is_probeable("wss://x")
    assert not rpc_pool._is_probeable("ftp://x")

    monkeypatch.setattr(rpc_pool, "_has_wss_support", lambda: True)
    assert rpc_pool._is_probeable("ws://x")
    assert rpc_pool._is_probeable("wss://x")


def test_registry_caches_within_ttl(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}
    payload = {"chainId": 1, "name": "Ethereum", "rpc": ["https://rpc.example"]}
    body = json.dumps(payload)

    def fake_get(url, timeout):
        calls["n"] += 1
        return DummyResp(body)

    monkeypatch.setattr(rpc_pool.httpx, "get", fake_get)

    reg = rpc_pool.ChainsRegistry(ttl_seconds=60)

    m1 = reg.get(1)
    m2 = reg.get(1)

    assert calls["n"] == 1
    assert m1 == m2
    assert m1.chain_id == 1
    assert m1.name == "Ethereum"
    assert m1.rpc == ["https://rpc.example"]


def test_registry_refreshes_after_ttl(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}
    now = {"t": 1000.0}

    def fake_time():
        return now["t"]

    p1 = json.dumps({"chainId": 1, "name": "A", "rpc": ["https://a"]})
    p2 = json.dumps({"chainId": 1, "name": "B", "rpc": ["https://b"]})

    def fake_get(url, timeout):
        calls["n"] += 1
        return DummyResp(p1 if calls["n"] == 1 else p2)

    monkeypatch.setattr(
        rpc_pool,
        "time",
        SimpleNamespace(time=fake_time, perf_counter=time.perf_counter, sleep=time.sleep),
    )
    monkeypatch.setattr(rpc_pool.httpx, "get", fake_get)

    reg = rpc_pool.ChainsRegistry(ttl_seconds=10)

    m1 = reg.get(1)
    now["t"] += 11
    m2 = reg.get(1)

    assert calls["n"] == 2
    assert m1.name == "A"
    assert m2.name == "B"


def test_probe_one_success(monkeypatch: pytest.MonkeyPatch, fake_make_transport):
    FakeTransport.behavior = {
        "https://ok": _batch_ok(chain_id=1, head_hex="0x2a"),
    }

    pr1 = rpc_pool._probe_one("https://ok", expected_chain_id=1, timeout_s=0.1)
    assert pr1.url == "https://ok"
    assert pr1.head == 0x2A
    assert pr1.rtt_ms >= 0.0


@pytest.mark.parametrize(
    "resp, err_substr",
    [
        ("not-a-list", "Non-batch response"),
        ([{"id": 1, "result": "0x1"}], "Missing response ids"),
        ([{"id": 1, "result": "0x1"}, {"id": 1, "result": "0x2"}], "Bad/duplicate id"),
        (_batch_ok(chain_id=2), "Wrong chainId"),
        ([{"id": 1, "result": "0x1"}, {"id": 2, "result": "potato"}], "Expected 0x-hex string"),
    ],
)
def test_probe_one_strict_failures(
    monkeypatch: pytest.MonkeyPatch, fake_make_transport, resp: Any, err_substr: str
):
    FakeTransport.behavior = {"https://bad": resp}

    with pytest.raises(Exception) as e:
        rpc_pool._probe_one("https://bad", expected_chain_id=1, timeout_s=0.1)

    assert err_substr in str(e.value)


def test_probe_urls_streaming_filters_and_dedups_http_only(
    monkeypatch: pytest.MonkeyPatch, fake_make_transport
):
    monkeypatch.setattr(rpc_pool, "_has_wss_support", lambda: False)
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u.strip().rstrip("/"))
    monkeypatch.setattr(rpc_pool, "normalize_target", lambda u: u)
    monkeypatch.setattr(
        rpc_pool,
        "is_url_target",
        lambda u: u.startswith(("http://", "https://", "ws://", "wss://")),
    )

    urls = [
        "https://ok/",
        "https://ok",
        "http://also-ok",
        "ws://nope",  # filtered (no wss support)
        "wss://nope2",  # filtered (no wss support)
        "https://templated/${KEY}",
        "not-a-url",
    ]

    FakeTransport.behavior = {
        "https://ok": _batch_ok(chain_id=1, head_hex="0x10"),
        "http://also-ok": _batch_ok(chain_id=1, head_hex="0x11"),
    }

    results = list(
        rpc_pool.probe_urls_streaming(
            urls,
            expected_chain_id=1,
            timeout_s=0.05,
            max_workers=4,
            deadline_s=0.05,
        )
    )

    got = sorted((r.url, r.head) for r in results)
    assert got == [("http://also-ok", 0x11), ("https://ok", 0x10)]


def test_probe_urls_streaming_includes_wss_when_supported(
    monkeypatch: pytest.MonkeyPatch, fake_make_transport
):
    monkeypatch.setattr(rpc_pool, "_has_wss_support", lambda: True)
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u.strip().rstrip("/"))
    monkeypatch.setattr(rpc_pool, "normalize_target", lambda u: u)
    monkeypatch.setattr(
        rpc_pool,
        "is_url_target",
        lambda u: u.startswith(("http://", "https://", "ws://", "wss://")),
    )

    urls = ["wss://ok/", "https://ok"]

    FakeTransport.behavior = {
        "wss://ok": _batch_ok(chain_id=1, head_hex="0x12"),
        "https://ok": _batch_ok(chain_id=1, head_hex="0x10"),
    }

    results = list(
        rpc_pool.probe_urls_streaming(
            urls,
            expected_chain_id=1,
            timeout_s=0.05,
            max_workers=2,
            deadline_s=0.05,
        )
    )
    got = sorted((r.url, r.head) for r in results)
    assert got == [("https://ok", 0x10), ("wss://ok", 0x12)]


def test_probe_urls_streaming_ignores_bad_responses(
    monkeypatch: pytest.MonkeyPatch, fake_make_transport
):
    monkeypatch.setattr(rpc_pool, "_has_wss_support", lambda: False)
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u)
    monkeypatch.setattr(rpc_pool, "normalize_target", lambda u: u)
    monkeypatch.setattr(
        rpc_pool,
        "is_url_target",
        lambda u: u.startswith(("http://", "https://", "ws://", "wss://")),
    )

    FakeTransport.behavior = {
        "https://ok": _batch_ok(chain_id=1, head_hex="0x10"),
        "https://wrong-chain": _batch_ok(chain_id=2, head_hex="0x10"),
        "https://non-batch": {"id": 1},
    }

    urls = ["https://ok", "https://wrong-chain", "https://non-batch"]

    results = list(
        rpc_pool.probe_urls_streaming(
            urls,
            expected_chain_id=1,
            timeout_s=0.05,
            max_workers=3,
            deadline_s=0.05,
        )
    )
    assert [r.url for r in results] == ["https://ok"]


def test_probe_urls_streaming_empty_candidates_returns_no_items(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rpc_pool, "_has_wss_support", lambda: False)
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u)
    monkeypatch.setattr(rpc_pool, "normalize_target", lambda u: u)
    monkeypatch.setattr(
        rpc_pool,
        "is_url_target",
        lambda u: u.startswith(("http://", "https://", "ws://", "wss://")),
    )
    urls = ["ws://x", "wss://y", "https://x/${KEY}"]
    assert list(rpc_pool.probe_urls_streaming(urls, expected_chain_id=1, deadline_s=0.05)) == []


def test_acquire_pool_manager_returns_shared_instance_and_refcounts(
    monkeypatch: pytest.MonkeyPatch,
):
    called = {"ensure": 0, "stop": 0}

    def _ensure() -> None:
        called["ensure"] += 1

    def _stop() -> None:
        called["stop"] += 1

    monkeypatch.setattr(rpc_pool, "_ensure_scheduler_running", _ensure)
    monkeypatch.setattr(rpc_pool, "_stop_scheduler_if_idle", _stop)

    pm1 = rpc_pool.acquire_pool_manager(1)
    pm2 = rpc_pool.acquire_pool_manager(1)
    pm3 = rpc_pool.acquire_pool_manager(2)

    assert pm1 is pm2
    assert pm1 is not pm3

    with rpc_pool._pool_lock:
        assert rpc_pool._pool_refcount[1] == 2
        assert rpc_pool._pool_refcount[2] == 1

    assert called["ensure"] == 3

    rpc_pool.release_pool_manager(1)
    with rpc_pool._pool_lock:
        assert rpc_pool._pool_refcount[1] == 1

    # Fully release => scheduler eligible to stop
    rpc_pool.release_pool_manager(1)
    rpc_pool.release_pool_manager(2)
    assert called["stop"] >= 1


def test_poolmanager_fill_until_target_pool():
    pm = rpc_pool.PoolManager(1, target_pool=2)

    pm._handle_probe_result(pr=pr("https://a", 100, 10), now=0.0)
    pm._handle_probe_result(pr=pr("https://b", 90, 11), now=0.0)

    assert pm.best_urls(10, await_first=False) == ["https://b", "https://a"]


def test_poolmanager_replace_worst_when_significantly_better():
    pm = rpc_pool.PoolManager(1, target_pool=2)

    pm._handle_probe_result(pr=pr("https://slow", 200, 10), now=0.0)
    pm._handle_probe_result(pr=pr("https://fast", 80, 10), now=0.0)
    assert set(pm.best_urls(10, await_first=False)) == {"https://slow", "https://fast"}

    pm._handle_probe_result(pr=pr("https://new", 50, 10), now=0.0)

    urls = pm.best_urls(10, await_first=False)
    assert "https://new" in urls
    assert "https://slow" not in urls


def test_poolmanager_replace_cooldown_blocks_replacement():
    pm = rpc_pool.PoolManager(1, target_pool=2)
    pm._state.next_replace_ts = 100.0

    pm._handle_probe_result(pr=pr("https://slow", 200, 10), now=0.0)
    pm._handle_probe_result(pr=pr("https://fast", 80, 10), now=0.0)

    pm._handle_probe_result(pr=pr("https://new", 50, 10), now=0.0)

    urls = pm.best_urls(10, await_first=False)
    assert "https://slow" in urls
    assert "https://new" not in urls


def test_poolmanager_eviction_cooldown_blocks_readd():
    pm = rpc_pool.PoolManager(1, target_pool=2)

    pm._cooldown_until["https://a"] = 100.0
    pm._handle_probe_result(pr=pr("https://a", 50, 10), now=0.0)

    assert pm.best_urls(10, await_first=False) == []


def test_poolmanager_health_check_eviction_on_lag(monkeypatch: pytest.MonkeyPatch):
    pm = rpc_pool.PoolManager(1, target_pool=2, max_lag_blocks=5)

    pm._handle_probe_result(pr=pr("https://a", 100, 100), now=0.0)
    assert pm.best_urls(10, await_first=False) == ["https://a"]

    pm._state.best_head = 100

    def fake_probe_one(url: str, **kwargs):
        return pr(url, 100, 80)  # lagging by 20

    monkeypatch.setattr(rpc_pool, "_probe_one", fake_probe_one)

    pm._state.next_health_ts = -1.0
    pm._health_check(now=0.0)

    assert pm.best_urls(10, await_first=False) == []


def test_poolmanager_health_check_eviction_on_failure(monkeypatch: pytest.MonkeyPatch):
    pm = rpc_pool.PoolManager(1, target_pool=2)

    pm._handle_probe_result(pr=pr("https://a", 100, 10), now=0.0)
    assert pm.best_urls(10, await_first=False) == ["https://a"]

    def fake_probe_one(url: str, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(rpc_pool, "_probe_one", fake_probe_one)

    pm._state.next_health_ts = -1.0
    pm._health_check(now=0.0)

    assert pm.best_urls(10, await_first=False) == []


def test_poolmanager_best_urls_empty_returns_immediately_when_not_awaiting():
    pm = rpc_pool.PoolManager(1, target_pool=2)
    assert pm.best_urls(5, await_first=False) == []


def test_poolmanager_best_urls_blocks_when_awaiting_until_ready_then_returns():
    pm = rpc_pool.PoolManager(1, target_pool=2)

    result: dict[str, object] = {}
    started = threading.Event()

    def run() -> None:
        started.set()
        result["urls"] = pm.best_urls(5, await_first=True)

    t = _REAL_THREAD(target=run, daemon=True)
    t.start()

    assert started.wait(timeout=0.2)

    # Should still be blocked
    t.join(timeout=0.05)
    assert t.is_alive()

    # Mark ready without adding active: should unblock and return []
    pm._ready.set()
    t.join(timeout=0.5)
    assert not t.is_alive()
    assert result["urls"] == []


def test_probe_urls_streaming_skips_missing_env_without_crashing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NOPE", raising=False)

    # This must not raise; it should just yield nothing.
    results = list(
        rpc_pool.probe_urls_streaming(
            ["https://example.com/$NOPE"],
            expected_chain_id=1,
            timeout_s=0.01,
            max_workers=1,
            deadline_s=0.05,
        )
    )
    assert results == []
