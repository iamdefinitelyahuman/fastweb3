# tests/integration/test_web3_e2e_httpx_integration.py
from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

import httpx
import pytest

from fw3 import Web3, Web3Config
from fw3.provider import Provider


@pytest.fixture(scope="module", autouse=True)
def _fast_probe_deadlines():
    """Speed up pool probing during tests by shortening probe deadlines."""
    import fw3.provider.pool as pool

    mp = pytest.MonkeyPatch()
    mp.setattr(pool, "PROBE_DEADLINE_MIN_S", 0.1)
    mp.setattr(pool, "PROBE_DEADLINE_MULTIPLIER", 0.0)
    yield
    mp.undo()


def _run_with_timeout(fn, *, timeout_s: float = 5.0):
    """Run fn() in a thread so a bug can't hang the entire test suite."""

    out: Dict[str, Any] = {}

    def runner() -> None:
        try:
            out["value"] = fn()
        except BaseException as exc:  # pragma: no cover
            out["exc"] = exc

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(timeout_s)

    assert not t.is_alive(), f"timed out after {timeout_s}s"

    if "exc" in out:
        raise out["exc"]
    return out.get("value")


@dataclass
class _PostCall:
    url: str
    payload: Any


class _HttpxMock:
    """Thread-safe httpx mock that simulates:

    - ChainsRegistry JSON fetch via httpx.get
    - JSON-RPC calls via httpx.Client.post

    Only network boundary is mocked (httpx). All library classes run real code.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.get_calls: List[str] = []
        self.post_calls: List[_PostCall] = []

        # chain_id -> chain metadata returned by the chains registry
        self.chain_meta: Dict[int, Dict[str, Any]] = {}

        # rpc_url -> behavior
        self.rpc_chain_id: Dict[str, int] = {}
        self.rpc_block_number: Dict[str, int] = {}
        self.rpc_gas_price: Dict[str, int] = {}
        self.rpc_syncing: Dict[str, Any] = {}

        # rpc_url -> status code for POST (defaults to 200)
        self.post_status: Dict[str, int] = {}

        # urls for which we intentionally return batch responses out-of-order
        self.reverse_batch_response_for: set[str] = set()

    def reset_calls(self) -> None:
        with self._lock:
            self.get_calls.clear()
            self.post_calls.clear()

    def add_chain(self, chain_id: int, *, rpc_urls: Sequence[str], name: str = "TestChain") -> None:
        self.chain_meta[int(chain_id)] = {
            "chainId": int(chain_id),
            "name": name,
            "rpc": list(rpc_urls),
        }

    def add_rpc_endpoint(
        self,
        url: str,
        *,
        chain_id: int,
        block_number: int = 123,
        gas_price: int = 1,
        syncing: Any = False,
    ) -> None:
        self.rpc_chain_id[str(url)] = int(chain_id)
        self.rpc_block_number[str(url)] = int(block_number)
        self.rpc_gas_price[str(url)] = int(gas_price)
        self.rpc_syncing[str(url)] = syncing

    def _extract_chain_id_from_registry_url(self, url: str) -> int:
        m = re.search(r"eip155-(\d+)\.json", url)
        if not m:
            raise AssertionError(f"unexpected chains registry url: {url!r}")
        return int(m.group(1))

    def handle_get(self, url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        url_s = str(url)
        with self._lock:
            self.get_calls.append(url_s)

        chain_id = self._extract_chain_id_from_registry_url(url_s)
        if chain_id not in self.chain_meta:
            raise AssertionError(f"no chain_meta configured for chain_id={chain_id}")

        body = json.dumps(self.chain_meta[chain_id]).encode()
        req = httpx.Request("GET", url_s)
        return httpx.Response(200, content=body, request=req)

    def _rpc_result(self, url: str, method: str, params: list[Any]) -> Any:
        if method == "eth_chainId":
            if url not in self.rpc_chain_id:
                raise AssertionError(f"no rpc_chain_id configured for {url}")
            return hex(self.rpc_chain_id[url])

        if method == "eth_blockNumber":
            if url not in self.rpc_block_number:
                raise AssertionError(f"no rpc_block_number configured for {url}")
            return hex(self.rpc_block_number[url])

        if method == "eth_gasPrice":
            if url not in self.rpc_gas_price:
                raise AssertionError(f"no rpc_gas_price configured for {url}")
            return hex(self.rpc_gas_price[url])

        if method == "eth_syncing":
            if url not in self.rpc_syncing:
                raise AssertionError(f"no rpc_syncing configured for {url}")
            return self.rpc_syncing[url]

        raise AssertionError(f"unhandled rpc method: {method!r} (url={url})")

    def handle_post(self, url: str, payload: Any) -> httpx.Response:
        url_s = str(url)
        with self._lock:
            self.post_calls.append(_PostCall(url=url_s, payload=payload))

        status = int(self.post_status.get(url_s, 200))
        req = httpx.Request("POST", url_s)

        if status >= 400:
            # Make raise_for_status() raise HTTPStatusError (TransportError mapping)
            return httpx.Response(status, content=b"", request=req)

        if isinstance(payload, dict):
            method = payload.get("method")
            params = payload.get("params") or []
            _id = payload.get("id")
            result = self._rpc_result(url_s, str(method), list(params))
            resp_obj = {"jsonrpc": "2.0", "id": _id, "result": result}
            return httpx.Response(200, json=resp_obj, request=req)

        if isinstance(payload, list):
            resp_list: List[Dict[str, Any]] = []
            for item in payload:
                method = item.get("method")
                params = item.get("params") or []
                _id = item.get("id")
                result = self._rpc_result(url_s, str(method), list(params))
                resp_list.append({"jsonrpc": "2.0", "id": _id, "result": result})

            if url_s in self.reverse_batch_response_for:
                resp_list = list(reversed(resp_list))

            return httpx.Response(200, json=resp_list, request=req)

        raise AssertionError(f"unexpected payload type: {type(payload).__name__}")


@pytest.fixture(scope="module")
def httpx_mock() -> _HttpxMock:
    """Patch httpx for the whole module so background maintainer threads never hit real network."""

    mock = _HttpxMock()
    mp = pytest.MonkeyPatch()

    def _fake_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        return mock.handle_get(url, *args, **kwargs)

    def _fake_post(self: httpx.Client, url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        # fastweb3 uses post(url, json=payload)
        payload = kwargs.get("json")
        if payload is None and args:
            # very defensive: allow payload passed positionally
            payload = args[0]
        return mock.handle_post(url, payload)

    mp.setattr(httpx, "get", _fake_get)
    mp.setattr(httpx.Client, "post", _fake_post)

    yield mock

    # Best-effort: stop any global pool scheduler threads before unpatching httpx
    # so nothing can accidentally hit the real network during teardown.
    try:
        import fw3.provider.pool as _rpc_pool

        stop = getattr(_rpc_pool, "_sched_stop", None)
        t = getattr(_rpc_pool, "_sched_thread", None)
        if stop is not None:
            stop.set()
        if t is not None and getattr(t, "is_alive", lambda: False)():
            if threading.current_thread() is not t:
                t.join(timeout=10.0)

        lock = getattr(_rpc_pool, "_pool_lock", None)
        if lock is not None:
            with lock:
                getattr(_rpc_pool, "_pool_by_chain", {}).clear()
                getattr(_rpc_pool, "_pool_refcount", {}).clear()
                setattr(_rpc_pool, "_sched_thread", None)
                # If it is an Event, clear for the next test module.
                if stop is not None and hasattr(stop, "clear"):
                    stop.clear()
    except Exception:
        # If the internal API changes, don't fail these integration tests.
        pass

    mp.undo()


@pytest.fixture(autouse=True)
def _clear_httpx_logs(httpx_mock: _HttpxMock):
    httpx_mock.reset_calls()
    yield


def _methods_in_payload(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        return [str(payload.get("method"))]
    if isinstance(payload, list):
        return [str(x.get("method")) for x in payload]
    return []


def _payload_is_probe(payload: Any) -> bool:
    # PoolManager probes with [eth_chainId, eth_blockNumber]
    ms = _methods_in_payload(payload)
    return ms[:2] == ["eth_chainId", "eth_blockNumber"]


def _payload_is_pool_call(payload: Any, method: str) -> bool:
    # Provider pool call batches as [eth_blockNumber, <method>]
    ms = _methods_in_payload(payload)
    return len(ms) >= 2 and ms[0] == "eth_blockNumber" and ms[1] == method


def _await_pool_ready(w3: Web3, *, timeout_s: float = 12.0) -> list[str]:
    """Block until a PoolManager has at least one active URL."""

    pm = getattr(w3.provider, "pool_manager", None)
    assert pm is not None, "expected Web3.provider.pool_manager to be set"

    urls = _run_with_timeout(lambda: pm.best_urls(1, await_first=True), timeout_s=timeout_s)
    assert isinstance(urls, list)
    assert urls, "pool manager became ready but returned no active URLs"
    return urls


def test_init_primary_only_and_make_calls(httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.delenv("FASTWEB3_POOL_MODE", raising=False)

    primary = "http://primary.local"
    httpx_mock.add_rpc_endpoint(
        primary, chain_id=123, block_number=9001, gas_price=42, syncing=False
    )

    w3 = Web3(primary_endpoint=primary)

    chain_id = int(w3.eth.chain_id())
    assert chain_id == 123

    # eth_syncing is explicitly routed to primary
    assert w3.eth.syncing() == False  # noqa: E712

    # Both requests should go to the primary URL, and be single JSON-RPC objects.
    primary_calls = [c for c in httpx_mock.post_calls if c.url == primary]
    assert len(primary_calls) == 2

    methods = [_methods_in_payload(c.payload) for c in primary_calls]
    assert methods == [["eth_chainId"], ["eth_syncing"]]

    assert all(isinstance(c.payload, dict) for c in primary_calls)

    w3.close()


def test_init_manual_endpoints_batches_pool_requests_and_reorders_batch(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.delenv("FASTWEB3_POOL_MODE", raising=False)

    internal = "http://internal.local"

    httpx_mock.add_rpc_endpoint(
        internal, chain_id=1, block_number=100, gas_price=500, syncing=False
    )

    # Intentionally return batch responses out-of-order to verify Endpoint reorders by id.
    httpx_mock.reverse_batch_response_for.add(internal)

    w3 = Web3(endpoints=[internal])

    gas_price = int(w3.eth.gas_price())
    assert gas_price == 500

    assert len([c for c in httpx_mock.post_calls if c.url == internal]) == 1
    call = [c for c in httpx_mock.post_calls if c.url == internal][0]
    assert call.url == internal
    assert isinstance(call.payload, list)

    # Provider batches pool calls as [eth_blockNumber, userCall]
    assert _methods_in_payload(call.payload) == ["eth_blockNumber", "eth_gasPrice"]

    w3.close()


def test_init_chain_id_only_discovers_pool_and_routes_call(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")

    chain_id = 13371337
    pool = "http://pool.local"

    httpx_mock.add_chain(chain_id, rpc_urls=[pool])
    httpx_mock.add_rpc_endpoint(
        pool, chain_id=chain_id, block_number=222, gas_price=999, syncing=False
    )

    w3 = Web3(chain_id)

    # First, wait for the pool to become usable by performing an actual pool-routed call.
    # Running the evaluation in a thread prevents a broken maintainer from hanging pytest.
    price = _run_with_timeout(lambda: int(w3.eth.gas_price()), timeout_s=12.0)
    assert price == 999

    # Chains registry was queried.
    assert any(f"eip155-{chain_id}.json" in u for u in httpx_mock.get_calls)

    # At least one probe happened to the pool URL (from the pool maintainer).
    assert any(c.url == pool and _payload_is_probe(c.payload) for c in httpx_mock.post_calls)

    # The actual Web3 call went through provider->endpoint->transport to pool URL.
    assert any(
        c.url == pool and _payload_is_pool_call(c.payload, "eth_gasPrice")
        for c in httpx_mock.post_calls
    )

    w3.close()


def test_init_chain_id_plus_primary_routes_primary_calls_to_primary(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")

    chain_id = 13371338
    pool = "http://pool2.local"
    primary = "http://primary2.local"

    httpx_mock.add_chain(chain_id, rpc_urls=[pool])
    httpx_mock.add_rpc_endpoint(
        pool, chain_id=chain_id, block_number=1000, gas_price=111, syncing=False
    )
    httpx_mock.add_rpc_endpoint(
        primary, chain_id=chain_id, block_number=1001, gas_price=222, syncing=False
    )

    w3 = Web3(chain_id, primary_endpoint=primary)

    _await_pool_ready(w3, timeout_s=12.0)

    # Ensure pool route uses pool endpoint
    price = _run_with_timeout(lambda: int(w3.eth.gas_price()), timeout_s=12.0)
    assert price == 111

    # Ensure primary-routed method hits primary endpoint
    assert w3.eth.syncing() == False  # noqa: E712

    assert any(
        c.url == pool and _payload_is_pool_call(c.payload, "eth_gasPrice")
        for c in httpx_mock.post_calls
    )

    # Depending on whether pool mode is enabled, Provider may batch primary calls.
    assert any(
        c.url == primary and "eth_syncing" in _methods_in_payload(c.payload)
        for c in httpx_mock.post_calls
    )

    w3.close()


def test_init_chain_id_plus_endpoints_failover_to_pool_manager(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")

    chain_id = 13371339
    internal_bad = "http://internal-bad.local"
    pool = "http://pool3.local"

    httpx_mock.add_chain(chain_id, rpc_urls=[pool])
    httpx_mock.add_rpc_endpoint(
        pool, chain_id=chain_id, block_number=500, gas_price=777, syncing=False
    )

    # Internal endpoint exists but always fails at the transport layer.
    httpx_mock.add_rpc_endpoint(
        internal_bad, chain_id=chain_id, block_number=1, gas_price=1, syncing=False
    )
    httpx_mock.post_status[internal_bad] = 500

    # Reduce sleep impact if this ever retries.
    cfg = Web3Config(desired_pool_size=2, retry_policy_pool=Web3Config().retry_policy_pool)
    w3 = Web3(chain_id, endpoints=[internal_bad], config=cfg)

    _await_pool_ready(w3, timeout_s=12.0)

    price = _run_with_timeout(lambda: int(w3.eth.gas_price()), timeout_s=12.0)
    assert price == 777

    # We should see a failure attempt against internal_bad, then success against pool.
    urls = [
        c.url for c in httpx_mock.post_calls if _payload_is_pool_call(c.payload, "eth_gasPrice")
    ]
    assert internal_bad in urls
    assert pool in urls

    w3.close()


def test_init_via_custom_provider_still_wires_through_endpoint_and_transport(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.delenv("FASTWEB3_POOL_MODE", raising=False)

    internal = "http://custom-provider.local"
    httpx_mock.add_rpc_endpoint(
        internal, chain_id=1, block_number=42, gas_price=4242, syncing=False
    )

    provider = Provider([internal])
    w3 = Web3(provider=provider)

    price = int(w3.eth.gas_price())
    assert price == 4242

    pcalls = [c for c in httpx_mock.post_calls if c.url == internal]
    assert len(pcalls) == 1
    assert pcalls[0].url == internal

    w3.close()


def test_env_per_chain_primary_in_split_mode_disables_pool_and_routes_pool_calls_to_primary(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    chain_id = 424242
    primary = "http://env-primary.local"

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "split")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINTS", f"{chain_id}={primary}")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)

    httpx_mock.add_rpc_endpoint(
        primary, chain_id=chain_id, block_number=9, gas_price=8080, syncing=False
    )

    w3 = Web3(chain_id)

    price = int(w3.eth.gas_price())
    assert price == 8080

    # Pool is disabled, so no chains registry discovery should happen for *this* chain.
    assert not any(f"eip155-{chain_id}.json" in u for u in httpx_mock.get_calls)

    primary_calls = [c for c in httpx_mock.post_calls if c.url == primary]
    assert len(primary_calls) == 1

    # With pool disabled, Provider will do a single-call request here.
    assert isinstance(primary_calls[0].payload, dict)
    assert _methods_in_payload(primary_calls[0].payload) == ["eth_gasPrice"]

    w3.close()


def test_init_no_args_uses_env_default_primary(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    """Covers the convenience path: Web3() with FASTWEB3_PRIMARY_ENDPOINT set."""

    primary = "http://env-default-primary.local"

    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINT", primary)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.delenv("FASTWEB3_POOL_MODE", raising=False)

    httpx_mock.add_rpc_endpoint(
        primary, chain_id=99, block_number=1, gas_price=12345, syncing=False
    )

    w3 = Web3()

    # eth_chainId routes through pool by default, but with no pool it should fall back to primary.
    assert int(w3.eth.chain_id()) == 99

    pcalls = [c for c in httpx_mock.post_calls if c.url == primary]
    assert len(pcalls) == 1
    assert _methods_in_payload(pcalls[0].payload) == ["eth_chainId"]

    w3.close()


def test_init_chain_id_endpoints_and_primary_prefers_internal_for_pool_and_primary_for_primary(
    httpx_mock: _HttpxMock, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)
    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")

    chain_id = 13371340
    internal = "http://internal4.local"
    pool = "http://pool4.local"
    primary = "http://primary4.local"

    httpx_mock.add_chain(chain_id, rpc_urls=[pool])

    httpx_mock.add_rpc_endpoint(
        internal, chain_id=chain_id, block_number=10, gas_price=333, syncing=False
    )
    httpx_mock.add_rpc_endpoint(
        pool, chain_id=chain_id, block_number=11, gas_price=111, syncing=False
    )
    httpx_mock.add_rpc_endpoint(
        primary, chain_id=chain_id, block_number=12, gas_price=222, syncing=False
    )

    w3 = Web3(chain_id, endpoints=[internal], primary_endpoint=primary)

    # Pool route should prefer internal endpoints (they are listed before pool-manager URLs).
    price = int(w3.eth.gas_price())
    assert price == 333

    # Primary-routed method always hits the primary endpoint.
    assert w3.eth.syncing() == False  # noqa: E712

    assert any(
        c.url == internal and _payload_is_pool_call(c.payload, "eth_gasPrice")
        for c in httpx_mock.post_calls
    )
    assert any(
        c.url == primary and "eth_syncing" in _methods_in_payload(c.payload)
        for c in httpx_mock.post_calls
    )

    w3.close()
