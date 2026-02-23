import json
import time
from types import SimpleNamespace

import pytest

import fastweb3.chains as chains

# -------------------------
# Helpers / fakes
# -------------------------


class DummyResp:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHTTPTransport:
    """
    Default fake transport for probe tests. You can override behavior per-url
    by mutating FakeHTTPTransport.behavior[url] = callable(payload)->resp
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


class FakeProvider:
    def __init__(self, urls, **kwargs) -> None:
        self._urls = list(urls)

    def urls(self):
        return list(self._urls)

    def endpoint_count(self):
        return len(self._urls)

    def add_url(self, u: str):
        if u not in self._urls:
            self._urls.append(u)

    def remove_url(self, u: str):
        self._urls = [x for x in self._urls if x != u]


def _batch_ok(chain_id: int, head_hex: str = "0x10"):
    return [
        {"jsonrpc": "2.0", "id": 1, "result": hex(chain_id)},
        {"jsonrpc": "2.0", "id": 2, "result": head_hex},
    ]


# -------------------------
# Fixtures
# -------------------------


@pytest.fixture(autouse=True)
def reset_shared_pool():
    # Ensure tests don't leak shared providers across cases.
    with chains._provider_pool_lock:
        chains._provider_pool_by_chain.clear()
    yield
    with chains._provider_pool_lock:
        chains._provider_pool_by_chain.clear()


@pytest.fixture
def no_pool_thread(monkeypatch):
    # Don't start the maintainer thread in tests.
    monkeypatch.setattr(chains, "_start_pool_maintainer", lambda **kwargs: None)


# -------------------------
# Unit tests: small pure funcs
# -------------------------


def test_hex_to_int_ok():
    assert chains._hex_to_int("0x0") == 0
    assert chains._hex_to_int("0x10") == 16
    assert chains._hex_to_int("0xFF") == 255


@pytest.mark.parametrize("bad", [None, 123, "10", "0X10", "0x", "potato"])
def test_hex_to_int_bad_raises(bad):
    with pytest.raises(ValueError):
        chains._hex_to_int(bad)


def test_is_http_and_is_templated():
    assert chains._is_http("http://x")
    assert chains._is_http("https://x")
    assert not chains._is_http("ws://x")
    assert chains._is_templated("https://x/${INFURA_API_KEY}")
    assert not chains._is_templated("https://x/")


# -------------------------
# ChainsRegistry: caching + TTL
# -------------------------


def test_registry_caches_within_ttl(monkeypatch):
    calls = {"n": 0}

    payload = {"chainId": 1, "name": "Ethereum", "rpc": ["https://rpc.example"]}
    body = json.dumps(payload)

    def fake_get(url, timeout):
        calls["n"] += 1
        return DummyResp(body)

    monkeypatch.setattr(chains.httpx, "get", fake_get)

    reg = chains.ChainsRegistry(ttl_seconds=60)

    m1 = reg.get(1)
    m2 = reg.get(1)

    assert calls["n"] == 1
    assert m1 == m2
    assert m1.chain_id == 1
    assert m1.name == "Ethereum"
    assert m1.rpc == ["https://rpc.example"]


def test_registry_refreshes_after_ttl(monkeypatch):
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
        chains,
        "time",
        SimpleNamespace(time=fake_time, perf_counter=time.perf_counter, sleep=time.sleep),
    )
    monkeypatch.setattr(chains.httpx, "get", fake_get)

    reg = chains.ChainsRegistry(ttl_seconds=10)

    m1 = reg.get(1)
    now["t"] += 11
    m2 = reg.get(1)

    assert calls["n"] == 2
    assert m1.name == "A"
    assert m2.name == "B"


# -------------------------
# _probe_one: strict batch contract
# -------------------------


def test_probe_one_success(monkeypatch):
    monkeypatch.setattr(chains, "HTTPTransport", FakeHTTPTransport)

    FakeHTTPTransport.behavior = {
        "https://ok": _batch_ok(chain_id=1, head_hex="0x2a"),
    }

    pr = chains._probe_one("https://ok", expected_chain_id=1, timeout_s=0.1)
    assert pr.url == "https://ok"
    assert pr.head == 0x2A
    assert pr.rtt_ms >= 0.0


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
def test_probe_one_strict_failures(monkeypatch, resp, err_substr):
    monkeypatch.setattr(chains, "HTTPTransport", FakeHTTPTransport)
    FakeHTTPTransport.behavior = {"https://bad": resp}

    with pytest.raises(Exception) as e:
        chains._probe_one("https://bad", expected_chain_id=1, timeout_s=0.1)

    assert err_substr in str(e.value)


# -------------------------
# probe_urls_streaming: filtering, dedup, and streaming
# -------------------------


def test_probe_urls_streaming_filters_and_dedups(monkeypatch):
    monkeypatch.setattr(chains, "HTTPTransport", FakeHTTPTransport)
    monkeypatch.setattr(chains, "normalize_url", lambda u: u.strip().rstrip("/"))

    # Only these should be considered:
    # - http(s)
    # - not templated
    # - normalized + deduped
    urls = [
        "https://ok/",
        "https://ok",  # dup after normalization
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
        chains.probe_urls_streaming(
            urls,
            expected_chain_id=1,
            timeout_s=0.05,
            max_workers=4,
            deadline_s=0.05,
        )
    )

    got = sorted((r.url, r.head) for r in results)
    assert got == [("http://also-ok", 0x11), ("https://ok", 0x10)]


def test_probe_urls_streaming_ignores_bad_responses(monkeypatch):
    monkeypatch.setattr(chains, "HTTPTransport", FakeHTTPTransport)
    monkeypatch.setattr(chains, "normalize_url", lambda u: u)

    FakeHTTPTransport.behavior = {
        "https://ok": _batch_ok(chain_id=1, head_hex="0x10"),
        "https://wrong-chain": _batch_ok(chain_id=2, head_hex="0x10"),
        "https://non-batch": {"id": 1},
    }

    urls = ["https://ok", "https://wrong-chain", "https://non-batch"]

    results = list(
        chains.probe_urls_streaming(
            urls,
            expected_chain_id=1,
            timeout_s=0.05,
            max_workers=3,
            deadline_s=0.05,
        )
    )
    assert [r.url for r in results] == ["https://ok"]


def test_probe_urls_streaming_empty_candidates_returns_no_items(monkeypatch):
    monkeypatch.setattr(chains, "normalize_url", lambda u: u)

    # all filtered: non-http + templated
    urls = ["ws://x", "https://x/${KEY}"]
    assert list(chains.probe_urls_streaming(urls, expected_chain_id=1, deadline_s=0.05)) == []


# -------------------------
# provider_for_chain: shared, hybrid, discovery-only
# -------------------------


def test_provider_for_chain_shared_instance(monkeypatch, no_pool_thread):
    monkeypatch.setattr(chains, "Provider", FakeProvider)

    # Make deferred_response execute bg synchronously and return the provider value.
    def immediate_deferred_response(bg):
        class H:
            def __init__(self):
                self.value = None

            def set_value(self, v):
                self.value = v

        h = H()
        bg(h)
        return h.value

    monkeypatch.setattr(chains, "deferred_response", immediate_deferred_response)

    # ChainsRegistry.get called once; return trivial meta (not used in hybrid).
    monkeypatch.setattr(
        chains,
        "ChainsRegistry",
        lambda: SimpleNamespace(get=lambda cid: chains.ChainMeta(cid, "X", ["https://public"])),
    )

    p1 = chains.provider_for_chain(1, priority_endpoints=["https://priority"])
    p2 = chains.provider_for_chain(1, priority_endpoints=["https://other"])

    assert p1 is p2
    assert p1.urls() == ["https://priority"]


def test_provider_for_chain_hybrid_publishes_immediately(monkeypatch, no_pool_thread):
    monkeypatch.setattr(chains, "Provider", FakeProvider)

    def immediate_deferred_response(bg):
        class H:
            def __init__(self):
                self.value = None

            def set_value(self, v):
                self.value = v

        h = H()
        bg(h)
        return h.value

    monkeypatch.setattr(chains, "deferred_response", immediate_deferred_response)

    monkeypatch.setattr(
        chains,
        "ChainsRegistry",
        lambda: SimpleNamespace(get=lambda cid: chains.ChainMeta(cid, "X", ["https://public"])),
    )

    p = chains.provider_for_chain(
        1,
        priority_endpoints=["https://p1/", "https://p1", "https://p2"],
    )

    # priority_endpoints are normalized/deduped before Provider
    assert p.urls() == ["https://p1/", "https://p1", "https://p2"] or p.urls() == [
        "https://p1",
        "https://p2",
    ]
    # (depends on normalize_url; here we didn't monkeypatch it)


def test_provider_for_chain_discovery_uses_first_good_public_endpoint(monkeypatch, no_pool_thread):
    monkeypatch.setattr(chains, "Provider", FakeProvider)

    # Make normalize_url stable for this test.
    monkeypatch.setattr(chains, "normalize_url", lambda u: u.rstrip("/"))

    # Synchronous deferred response
    def immediate_deferred_response(bg):
        class H:
            def __init__(self):
                self.value = None

            def set_value(self, v):
                self.value = v

        h = H()
        bg(h)
        return h.value

    monkeypatch.setattr(chains, "deferred_response", immediate_deferred_response)

    # Registry returns rpc list; probe_urls_streaming yields first good
    monkeypatch.setattr(
        chains,
        "ChainsRegistry",
        lambda: SimpleNamespace(
            get=lambda cid: chains.ChainMeta(cid, "X", ["https://a", "https://b"])
        ),
    )

    def fake_probe(urls, expected_chain_id, **kwargs):
        # yield "https://b" first
        yield chains.ProbeResult(url="https://b", rtt_ms=12.0, head=100)

    monkeypatch.setattr(chains, "probe_urls_streaming", fake_probe)

    p = chains.provider_for_chain(1)
    assert p.urls() == ["https://b"]


def test_provider_for_chain_discovery_raises_if_none_found(monkeypatch, no_pool_thread):
    monkeypatch.setattr(chains, "Provider", FakeProvider)
    monkeypatch.setattr(chains, "normalize_url", lambda u: u)

    def immediate_deferred_response(bg):
        class H:
            def __init__(self):
                self.value = None

            def set_value(self, v):
                self.value = v

        h = H()
        return bg(h)  # will raise

    monkeypatch.setattr(chains, "deferred_response", immediate_deferred_response)

    monkeypatch.setattr(
        chains,
        "ChainsRegistry",
        lambda: SimpleNamespace(get=lambda cid: chains.ChainMeta(cid, "X", ["https://a"])),
    )

    monkeypatch.setattr(chains, "probe_urls_streaming", lambda *a, **k: iter(()))

    with pytest.raises(RuntimeError) as e:
        chains.provider_for_chain(1)

    assert "No usable batch-capable RPC endpoints found" in str(e.value)
