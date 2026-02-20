# src/fastweb3/chains.py
from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass
from typing import Iterator, Optional, Sequence

import httpx

from .deferred import Handle, deferred_response
from .provider import Provider
from .transport.http import HTTPTransport, HTTPTransportConfig

CHAINS_URL = (
    "https://raw.githubusercontent.com/ethereum-lists/chains/master/_data/chains/"
    "eip155-{chain_id}.json"
)


@dataclass(frozen=True)
class ChainMeta:
    chain_id: int
    name: str
    rpc: list[str]


class ChainsRegistry:
    """
    Fetches chain metadata (incl. public RPC URLs) from ethereum-lists/chains.

    Notes:
      - Sync (httpx.get)
      - Simple TTL cache
    """

    def __init__(self, *, ttl_seconds: int = 24 * 3600) -> None:
        self._ttl = ttl_seconds
        self._cache: dict[int, tuple[float, ChainMeta]] = {}
        self._lock = threading.Lock()

    def get(self, chain_id: int) -> ChainMeta:
        now = time.time()

        with self._lock:
            cached = self._cache.get(chain_id)
            if cached and (now - cached[0]) < self._ttl:
                return cached[1]

        url = CHAINS_URL.format(chain_id=chain_id)
        r = httpx.get(url, timeout=10.0)
        r.raise_for_status()
        data = json.loads(r.text)

        meta = ChainMeta(
            chain_id=int(data["chainId"]),
            name=str(data.get("name", "")),
            rpc=list(data.get("rpc", [])),
        )

        with self._lock:
            self._cache[chain_id] = (now, meta)

        return meta


@dataclass(frozen=True)
class ProbeResult:
    url: str
    rtt_ms: float
    head: int


def _is_http(url: str) -> bool:
    return url.startswith("http://") or url.startswith("https://")


def _is_templated(url: str) -> bool:
    # Common in the chains list: ".../${INFURA_API_KEY}" etc.
    return "${" in url


def _hex_to_int(x: object) -> int:
    if not isinstance(x, str) or not x.startswith("0x"):
        raise ValueError(f"Expected 0x-hex string, got {x!r}")
    return int(x, 16)


def probe_urls_streaming(
    urls: list[str],
    *,
    expected_chain_id: int,
    timeout_s: float = 1.5,
    max_workers: int = 32,
) -> Iterator[ProbeResult]:
    """
    Probe candidate RPC URLs and stream back successes as they arrive.

    Contract (strict):
      - endpoint must accept JSON-RPC batch payload (array) and return an array
      - must answer eth_chainId == expected_chain_id
      - must answer eth_blockNumber (parsed as int)
      - records RTT

    This is intentionally strict because fastweb3 will rely heavily on batching.
    """

    candidates = [u for u in urls if _is_http(u) and not _is_templated(u)]
    out: "queue.Queue[Optional[ProbeResult]]" = queue.Queue()

    # Batch probe: validate both chain and head.
    payload = [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber", "params": []},
    ]
    expected_hex = hex(expected_chain_id).lower()

    cfg = HTTPTransportConfig(
        timeout=timeout_s,
        connect_timeout=timeout_s,
        read_timeout=timeout_s,
        write_timeout=timeout_s,
        pool_timeout=timeout_s,
        # keep connections modest during probing; this is a one-shot burst
        max_connections=max(50, max_workers),
        max_keepalive_connections=min(20, max_workers),
        keepalive_expiry=10.0,
    )

    sem = threading.Semaphore(max_workers)
    threads: list[threading.Thread] = []

    def worker(u: str) -> None:
        t0 = time.perf_counter()
        tr = HTTPTransport(u, config=cfg)
        try:
            resp = tr.send(payload)

            # Strict: must be a list[dict]
            if not isinstance(resp, list) or not all(isinstance(x, dict) for x in resp):
                return

            by_id: dict[int, dict] = {}
            for item in resp:
                _id = item.get("id")
                if not isinstance(_id, int):
                    return
                if _id in by_id:
                    return
                by_id[_id] = item

            if 1 not in by_id or 2 not in by_id:
                return

            chain = by_id[1].get("result")
            if not isinstance(chain, str) or chain.lower() != expected_hex:
                return

            head_hex = by_id[2].get("result")
            head = _hex_to_int(head_hex)

            rtt_ms = (time.perf_counter() - t0) * 1000.0
            out.put(ProbeResult(url=u, rtt_ms=rtt_ms, head=head))
        except Exception:
            return
        finally:
            tr.close()

    def launch(u: str) -> None:
        sem.acquire()

        def run() -> None:
            try:
                worker(u)
            finally:
                sem.release()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        threads.append(t)

    for u in candidates:
        launch(u)

    def joiner() -> None:
        for t in threads:
            t.join()
        out.put(None)

    threading.Thread(target=joiner, daemon=True).start()

    while True:
        item = out.get()
        if item is None:
            break
        yield item


def provider_for_chain(
    chain_id: int,
    *,
    priority_endpoints: Optional[Sequence[str]] = None,
    target_pool: int = 6,
    max_lag_blocks: int = 8,
    probe_timeout_s: float = 1.5,
    probe_workers: int = 32,
) -> Provider:
    """
    Returns a Provider proxy (deferred) for a given chain_id.

    Modes:
      - Discovery-only: provider becomes ready when the first usable public endpoint is found.
      - Hybrid: if priority_endpoints are provided, provider is published immediately with those
        endpoints (priority for writes), then public endpoints are discovered+added in background.

    Probing is strict and batch-based: [eth_chainId, eth_blockNumber].
    Endpoints that are too far behind the best observed head are dropped.
    """

    priority_urls = list(priority_endpoints or [])

    def bg(h: Handle) -> None:
        reg = ChainsRegistry()
        meta = reg.get(chain_id)

        provider: Provider | None = None
        best_head: int | None = None

        # Hybrid mode: publish immediately with user endpoints as primary.
        # (We do not validate these yet; discovery is still used to add reliable public fallbacks.)
        if priority_urls:
            provider = Provider(priority_urls)
            h.set_value(provider)  # READY NOW (user endpoints)

        # Probe public endpoints and grow the pool over time.
        for pr in probe_urls_streaming(
            meta.rpc,
            expected_chain_id=chain_id,
            timeout_s=probe_timeout_s,
            max_workers=probe_workers,
        ):
            best_head = pr.head if best_head is None else max(best_head, pr.head)

            # Drop nodes that are materially behind our best observed head.
            if best_head is not None and (best_head - pr.head) > max_lag_blocks:
                continue

            if provider is None:
                provider = Provider([pr.url])
                h.set_value(provider)  # READY NOW (first good public endpoint)
            else:
                provider.add_url(pr.url, priority=False)

            if provider.endpoint_count() >= max(target_pool, len(priority_urls)):
                break

        # If we never published a provider at all, discovery fully failed.
        if provider is None:
            raise RuntimeError(
                f"No usable batch-capable RPC endpoints found for chain_id={chain_id}"
            )

    return deferred_response(bg)
