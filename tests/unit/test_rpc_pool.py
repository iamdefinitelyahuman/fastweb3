# tests/unit/test_rpc_pool.py
from __future__ import annotations

import json
import time
from types import SimpleNamespace
from typing import Any

import pytest

import fastweb3.rpc_pool as rpc_pool


class DummyResp:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHTTPTransport:
    """
    Default fake transport for probe tests.

    Set FakeHTTPTransport.behavior[url] to either:
      - a response object to return from send(payload)
      - a callable(payload)->response
      - or omit to raise
    """

    behavior: dict[str, object] = {}

    def __init__(self, url: str, config=None) -> None:
        self.url = url
        self.config = config
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


class FakeThread:
    """Prevents PoolManager background thread from starting in tests."""

    def __init__(self, target=None, daemon=None):
        self.target = target
        self.daemon = daemon
        self.started = False

    def start(self):
        self.started = True
        # Intentionally do not run the target.


def pr(url: str, rtt: float, head: int) -> rpc_pool.ProbeResult:
    return rpc_pool.ProbeResult(url=url, rtt_ms=rtt, head=head)


@pytest.fixture(autouse=True)
def reset_shared_pool_manager_registry():
    with rpc_pool._pool_lock:
        rpc_pool._pool_by_chain.clear()
    yield
    with rpc_pool._pool_lock:
        rpc_pool._pool_by_chain.clear()


@pytest.fixture
def no_pool_thread(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rpc_pool.threading, "Thread", FakeThread)


def test_hex_to_int_ok():
    assert rpc_pool._hex_to_int("0x0") == 0
    assert rpc_pool._hex_to_int("0x10") == 16
    assert rpc_pool._hex_to_int("0xFF") == 255


@pytest.mark.parametrize("bad", [None, 123, "10", "0X10", "0x", "potato"])
def test_hex_to_int_bad_raises(bad):
    with pytest.raises(ValueError):
        rpc_pool._hex_to_int(bad)


def test_is_http_and_is_templated():
    assert rpc_pool._is_http("http://x")
    assert rpc_pool._is_http("https://x")
    assert not rpc_pool._is_http("ws://x")
    assert rpc_pool._is_templated("https://x/${INFURA_API_KEY}")
    assert not rpc_pool._is_templated("https://x/")


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


def test_probe_one_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rpc_pool, "HTTPTransport", FakeHTTPTransport)

    FakeHTTPTransport.behavior = {
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
def test_probe_one_strict_failures(monkeypatch: pytest.MonkeyPatch, resp: Any, err_substr: str):
    monkeypatch.setattr(rpc_pool, "HTTPTransport", FakeHTTPTransport)
    FakeHTTPTransport.behavior = {"https://bad": resp}

    with pytest.raises(Exception) as e:
        rpc_pool._probe_one("https://bad", expected_chain_id=1, timeout_s=0.1)

    assert err_substr in str(e.value)


def test_probe_urls_streaming_filters_and_dedups(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rpc_pool, "HTTPTransport", FakeHTTPTransport)
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u.strip().rstrip("/"))

    urls = [
        "https://ok/",
        "https://ok",
        "http://also-ok",
        "ws://nope",
        "https://templated/${KEY}",
        "not-a-url",
    ]

    FakeHTTPTransport.behavior = {
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


def test_probe_urls_streaming_ignores_bad_responses(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rpc_pool, "HTTPTransport", FakeHTTPTransport)
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u)

    FakeHTTPTransport.behavior = {
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
    monkeypatch.setattr(rpc_pool, "normalize_url", lambda u: u)
    urls = ["ws://x", "https://x/${KEY}"]
    assert list(rpc_pool.probe_urls_streaming(urls, expected_chain_id=1, deadline_s=0.05)) == []


def test_get_pool_manager_returns_shared_instance(monkeypatch: pytest.MonkeyPatch, no_pool_thread):
    pm1 = rpc_pool.get_pool_manager(1)
    pm2 = rpc_pool.get_pool_manager(1)
    pm3 = rpc_pool.get_pool_manager(2)
    assert pm1 is pm2
    assert pm1 is not pm3


def test_poolmanager_fill_until_target_pool(no_pool_thread):
    pm = rpc_pool.PoolManager(1, target_pool=2)
    state = rpc_pool._MaintainerState()

    pm._handle_probe_result(state=state, pr=pr("https://a", 100, 10), now=0.0)
    pm._handle_probe_result(state=state, pr=pr("https://b", 90, 11), now=0.0)

    assert pm.best_urls(10) == ["https://b", "https://a"]


def test_poolmanager_replace_worst_when_significantly_better(no_pool_thread):
    pm = rpc_pool.PoolManager(1, target_pool=2)
    state = rpc_pool._MaintainerState()

    pm._handle_probe_result(state=state, pr=pr("https://slow", 200, 10), now=0.0)
    pm._handle_probe_result(state=state, pr=pr("https://fast", 80, 10), now=0.0)
    assert set(pm.best_urls(10)) == {"https://slow", "https://fast"}

    pm._handle_probe_result(state=state, pr=pr("https://new", 50, 10), now=0.0)

    urls = pm.best_urls(10)
    assert "https://new" in urls
    assert "https://slow" not in urls


def test_poolmanager_replace_cooldown_blocks_replacement(no_pool_thread):
    pm = rpc_pool.PoolManager(1, target_pool=2)
    state = rpc_pool._MaintainerState(next_replace_ts=100.0)

    pm._handle_probe_result(state=state, pr=pr("https://slow", 200, 10), now=0.0)
    pm._handle_probe_result(state=state, pr=pr("https://fast", 80, 10), now=0.0)

    pm._handle_probe_result(state=state, pr=pr("https://new", 50, 10), now=0.0)

    urls = pm.best_urls(10)
    assert "https://slow" in urls
    assert "https://new" not in urls


def test_poolmanager_eviction_cooldown_blocks_readd(no_pool_thread):
    pm = rpc_pool.PoolManager(1, target_pool=2)
    state = rpc_pool._MaintainerState()

    # Put url on eviction cooldown
    pm._cooldown_until["https://a"] = 100.0

    pm._handle_probe_result(state=state, pr=pr("https://a", 50, 10), now=0.0)

    # IMPORTANT: best_urls() blocks until pool is non-empty at least once.
    # Here the pool stays empty by design, so assert via internal state.
    with pm._lock:
        assert pm._active == []
        assert pm._active_set == set()


def test_poolmanager_health_check_eviction_on_lag(monkeypatch: pytest.MonkeyPatch, no_pool_thread):
    pm = rpc_pool.PoolManager(1, target_pool=2, max_lag_blocks=5)
    state = rpc_pool._MaintainerState()

    pm._handle_probe_result(state=state, pr=pr("https://a", 100, 100), now=0.0)
    assert pm.best_urls(10) == ["https://a"]

    pm._best_head = 100

    def fake_probe_one(url: str, **kwargs):
        return pr(url, 100, 80)  # lagging by 20

    monkeypatch.setattr(rpc_pool, "_probe_one", fake_probe_one)

    # Force health check to run now
    state.next_health_ts = -1.0
    pm._health_check(state=state, meta=rpc_pool.ChainMeta(1, "X", ["https://a"]), now=0.0)

    assert pm.best_urls(10) == []


def test_poolmanager_health_check_eviction_on_failure(
    monkeypatch: pytest.MonkeyPatch, no_pool_thread
):
    pm = rpc_pool.PoolManager(1, target_pool=2)
    state = rpc_pool._MaintainerState()

    pm._handle_probe_result(state=state, pr=pr("https://a", 100, 10), now=0.0)
    assert pm.best_urls(10) == ["https://a"]

    def fake_probe_one(url: str, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(rpc_pool, "_probe_one", fake_probe_one)

    state.next_health_ts = -1.0
    pm._health_check(state=state, meta=rpc_pool.ChainMeta(1, "X", ["https://a"]), now=0.0)

    assert pm.best_urls(10) == []
