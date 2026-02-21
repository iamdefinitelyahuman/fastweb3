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
from .provider import Provider, RetryPolicy
from .transport.http import HTTPTransport, HTTPTransportConfig
from .utils import normalize_url

CHAINS_URL = (
    "https://raw.githubusercontent.com/ethereum-lists/chains/master/_data/chains/"
    "eip155-{chain_id}.json"
)


POOL_EPOCH_SLEEP_S = 15 * 60  # how often we rescan the public list
POOL_HEALTH_INTERVAL_S = 60.0  # how often we probe active endpoints
POOL_IMPROVE_FRACTION = 0.20  # candidate must be 20% faster to replace
POOL_REPLACE_COOLDOWN_S = 10.0  # minimum seconds between replacements
POOL_EVICTION_COOLDOWN_S = 15 * 60  # don't re-add recently evicted URLs

# Probe safety net: probe pass terminates within a bounded wall-clock time.
# This prevents discovery from hanging forever if transports wedge.
PROBE_DEADLINE_MULTIPLIER = 4.0
PROBE_DEADLINE_MIN_S = 5.0


_provider_pool_lock = threading.Lock()
_provider_pool_by_chain: dict[int, Provider] = {}


def _get_shared_provider(chain_id: int) -> Provider | None:
    with _provider_pool_lock:
        return _provider_pool_by_chain.get(chain_id)


def _set_shared_provider(chain_id: int, provider_proxy: Provider) -> Provider:
    with _provider_pool_lock:
        existing = _provider_pool_by_chain.get(chain_id)
        if existing is not None:
            return existing
        _provider_pool_by_chain[chain_id] = provider_proxy
        return provider_proxy


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


_PROBE_PAYLOAD = [
    {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
    {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber", "params": []},
]


def _probe_one(
    url: str,
    *,
    expected_chain_id: int,
    timeout_s: float,
) -> ProbeResult:
    """
    Probe a single URL using the strict batch contract.
    Raises on any mismatch / batch violation / transport error.
    """
    expected_hex = hex(expected_chain_id).lower()

    cfg = HTTPTransportConfig(
        timeout=timeout_s,
        connect_timeout=timeout_s,
        read_timeout=timeout_s,
        write_timeout=timeout_s,
        pool_timeout=timeout_s,
        max_connections=10,
        max_keepalive_connections=5,
        keepalive_expiry=10.0,
    )

    t0 = time.perf_counter()
    tr = HTTPTransport(url, config=cfg)
    try:
        resp = tr.send(_PROBE_PAYLOAD)

        if not isinstance(resp, list) or not all(isinstance(x, dict) for x in resp):
            raise RuntimeError("Non-batch response")

        by_id: dict[int, dict] = {}
        for item in resp:
            _id = item.get("id")
            if not isinstance(_id, int) or _id in by_id:
                raise RuntimeError("Bad/duplicate id")
            by_id[_id] = item

        if 1 not in by_id or 2 not in by_id:
            raise RuntimeError("Missing response ids")

        chain = by_id[1].get("result")
        if not isinstance(chain, str) or chain.lower() != expected_hex:
            raise RuntimeError("Wrong chainId")

        head_hex = by_id[2].get("result")
        head = _hex_to_int(head_hex)

        rtt_ms = (time.perf_counter() - t0) * 1000.0
        return ProbeResult(url=url, rtt_ms=rtt_ms, head=head)
    finally:
        tr.close()


def probe_urls_streaming(
    urls: list[str],
    *,
    expected_chain_id: int,
    timeout_s: float = 1.5,
    max_workers: int = 32,
    deadline_s: float | None = None,
) -> Iterator[ProbeResult]:
    """
    Probe candidate RPC URLs and stream back successes as they arrive.

    Contract (strict):
      - endpoint must accept JSON-RPC batch payload (array) and return an array
      - must answer eth_chainId == expected_chain_id
      - must answer eth_blockNumber (parsed as int)
      - records RTT
    """

    # Dedup + normalize upfront so we don't waste probe workers.
    seen: set[str] = set()
    candidates: list[str] = []
    for u in urls:
        if not _is_http(u) or _is_templated(u):
            continue
        nu = normalize_url(u)
        if nu in seen:
            continue
        seen.add(nu)
        candidates.append(nu)

    if not candidates:
        return
        yield  # unreachable, keeps typing happy

    if deadline_s is None:
        deadline_s = max(PROBE_DEADLINE_MIN_S, PROBE_DEADLINE_MULTIPLIER * float(timeout_s))
    deadline = time.perf_counter() + float(deadline_s)

    work_q: "queue.Queue[Optional[str]]" = queue.Queue()
    out_q: "queue.Queue[ProbeResult]" = queue.Queue()

    for u in candidates:
        work_q.put(u)

    worker_count = max(1, min(int(max_workers), len(candidates)))
    for _ in range(worker_count):
        work_q.put(None)

    expected_hex = hex(expected_chain_id).lower()

    cfg = HTTPTransportConfig(
        timeout=timeout_s,
        connect_timeout=timeout_s,
        read_timeout=timeout_s,
        write_timeout=timeout_s,
        pool_timeout=timeout_s,
        # keep connections modest during probing; this is a burst
        max_connections=max(50, worker_count),
        max_keepalive_connections=min(20, worker_count),
        keepalive_expiry=10.0,
    )

    def worker() -> None:
        while True:
            u = work_q.get()
            if u is None:
                return

            t0 = time.perf_counter()
            tr = HTTPTransport(u, config=cfg)
            try:
                resp = tr.send(_PROBE_PAYLOAD)

                if not isinstance(resp, list) or not all(isinstance(x, dict) for x in resp):
                    continue

                by_id: dict[int, dict] = {}
                ok = True
                for item in resp:
                    _id = item.get("id")
                    if not isinstance(_id, int) or _id in by_id:
                        ok = False
                        break
                    by_id[_id] = item

                if not ok or 1 not in by_id or 2 not in by_id:
                    continue

                chain = by_id[1].get("result")
                if not isinstance(chain, str) or chain.lower() != expected_hex:
                    continue

                head_hex = by_id[2].get("result")
                head = _hex_to_int(head_hex)

                rtt_ms = (time.perf_counter() - t0) * 1000.0
                out_q.put(ProbeResult(url=u, rtt_ms=rtt_ms, head=head))
            except Exception:
                continue
            finally:
                tr.close()

    for _ in range(worker_count):
        threading.Thread(target=worker, daemon=True).start()

    # Stream successes until deadline. We do NOT join workers; a wedged transport
    # could prevent joins and hang the generator.
    while True:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            break
        try:
            item = out_q.get(timeout=min(0.2, remaining))
        except queue.Empty:
            continue
        yield item


def _start_pool_maintainer(
    *,
    provider: Provider,
    meta: ChainMeta,
    chain_id: int,
    pinned: set[str],
    target_pool: int,
    max_lag_blocks: int,
    probe_timeout_s: float,
    probe_workers: int,
) -> None:
    """
    Start a daemon thread that continuously improves/repairs the provider pool.
    This is intentionally decoupled from deferred.Handle completion semantics.
    """

    def maintain() -> None:
        best_head: int | None = None
        rtt_by_url: dict[str, float] = {}
        cooldown_until: dict[str, float] = {}

        next_health_ts = time.time() + POOL_HEALTH_INTERVAL_S
        next_replace_ts = 0.0

        def desired_active_count() -> int:
            return max(target_pool, len(pinned))

        def non_pinned_active_urls() -> list[str]:
            return [u for u in provider.urls() if u not in pinned]

        def try_fill(url: str) -> None:
            # Provider handles normalization+dedup internally.
            if url not in provider.urls():
                provider.add_url(url)

        def evict(url: str) -> None:
            if url in pinned:
                return
            provider.remove_url(url)
            cooldown_until[url] = time.time() + POOL_EVICTION_COOLDOWN_S

        def health_check() -> None:
            nonlocal next_health_ts, best_head
            now = time.time()
            if now < next_health_ts:
                return
            next_health_ts = now + POOL_HEALTH_INTERVAL_S

            for u in list(provider.urls()):
                if cooldown_until.get(u, 0.0) > now:
                    continue
                try:
                    pr = _probe_one(
                        u,
                        expected_chain_id=chain_id,
                        timeout_s=min(1.0, probe_timeout_s),
                    )
                    rtt_by_url[u] = pr.rtt_ms
                    best_head = pr.head if best_head is None else max(best_head, pr.head)

                    if best_head is not None and (best_head - pr.head) > max_lag_blocks:
                        if u not in pinned:
                            evict(u)
                except Exception:
                    if u not in pinned:
                        evict(u)

        while True:
            # One epoch: scan public list once (bounded by probe_urls_streaming deadline)
            for pr in probe_urls_streaming(
                meta.rpc,
                expected_chain_id=chain_id,
                timeout_s=probe_timeout_s,
                max_workers=probe_workers,
            ):
                health_check()

                now = time.time()
                if now < next_replace_ts:
                    continue
                if cooldown_until.get(pr.url, 0.0) > now:
                    continue

                best_head = pr.head if best_head is None else max(best_head, pr.head)
                if best_head is not None and (best_head - pr.head) > max_lag_blocks:
                    continue

                rtt_by_url[pr.url] = pr.rtt_ms

                if provider.endpoint_count() < desired_active_count():
                    try_fill(pr.url)
                    continue

                act = non_pinned_active_urls()
                if not act:
                    continue

                def rtt(u: str) -> float:
                    return rtt_by_url.get(u, float("inf"))

                victim = max(act, key=rtt)
                victim_rtt = rtt(victim)

                # Replace only if meaningfully better to avoid churn.
                if pr.rtt_ms < victim_rtt * (1.0 - POOL_IMPROVE_FRACTION):
                    evict(victim)
                    try_fill(pr.url)
                    next_replace_ts = now + POOL_REPLACE_COOLDOWN_S

            # Sleep between epochs, but keep health checks running.
            sleep_end = time.time() + POOL_EPOCH_SLEEP_S
            while time.time() < sleep_end:
                health_check()
                time.sleep(5)

    threading.Thread(target=maintain, daemon=True).start()


def provider_for_chain(
    chain_id: int,
    *,
    priority_endpoints: Optional[Sequence[str]] = None,
    target_pool: int = 6,
    max_lag_blocks: int = 8,
    probe_timeout_s: float = 1.5,
    probe_workers: int = 32,
    retry_policy_read: Optional[RetryPolicy] = None,
    retry_policy_write: Optional[RetryPolicy] = None,
) -> Provider:
    """
    Returns a Provider proxy (deferred) for a given chain_id.

    Modes:
      - Discovery-only: provider becomes ready when the first usable public endpoint is found.
      - Hybrid: if priority_endpoints are provided, provider is published immediately with those
        endpoints (priority for writes), then public endpoints are discovered+added in background.

    Probing is strict and batch-based: [eth_chainId, eth_blockNumber].
    Endpoints that are too far behind the best observed head are dropped.

    Shared pools:
      - The first call for a given chain_id creates the background discovery thread.
      - Subsequent calls return the same deferred Provider proxy immediately.
      - Configuration is "first call wins" per chain_id.
    """

    # Shared pool: if already created, return immediately (no duplicate probing).
    shared = _get_shared_provider(chain_id)
    if shared is not None:
        return shared

    # Dedup + normalize user endpoints before creating the Provider
    priority_urls: list[str] = []
    seen_priority: set[str] = set()
    for u in priority_endpoints or []:
        nu = normalize_url(u)
        if nu in seen_priority:
            continue
        seen_priority.add(nu)
        priority_urls.append(nu)

    pinned = set(priority_urls)

    def bg(h: Handle) -> None:
        reg = ChainsRegistry()
        meta = reg.get(chain_id)

        provider: Provider | None = None
        best_head: int | None = None

        # HYBRID: publish immediately and return quickly.
        if priority_urls:
            provider = Provider(
                priority_urls,
                retry_policy_read=retry_policy_read,
                retry_policy_write=retry_policy_write,
            )
            h.set_value(provider)  # READY NOW

            _start_pool_maintainer(
                provider=provider,
                meta=meta,
                chain_id=chain_id,
                pinned=pinned,
                target_pool=target_pool,
                max_lag_blocks=max_lag_blocks,
                probe_timeout_s=probe_timeout_s,
                probe_workers=probe_workers,
            )
            return

        # DISCOVERY-ONLY: find the first usable public endpoint and return quickly.
        deadline_s = max(PROBE_DEADLINE_MIN_S, PROBE_DEADLINE_MULTIPLIER * float(probe_timeout_s))
        for pr in probe_urls_streaming(
            meta.rpc,
            expected_chain_id=chain_id,
            timeout_s=probe_timeout_s,
            max_workers=probe_workers,
            deadline_s=deadline_s,
        ):
            best_head = pr.head if best_head is None else max(best_head, pr.head)

            if best_head is not None and (best_head - pr.head) > max_lag_blocks:
                continue

            provider = Provider(
                [pr.url],
                retry_policy_read=retry_policy_read,
                retry_policy_write=retry_policy_write,
            )
            h.set_value(provider)  # READY NOW (first good public endpoint)

            _start_pool_maintainer(
                provider=provider,
                meta=meta,
                chain_id=chain_id,
                pinned=pinned,
                target_pool=target_pool,
                max_lag_blocks=max_lag_blocks,
                probe_timeout_s=probe_timeout_s,
                probe_workers=probe_workers,
            )
            return

        raise RuntimeError(f"No usable batch-capable RPC endpoints found for chain_id={chain_id}")

    # Create once; if another thread raced us, return the already-registered one.
    proxy = deferred_response(bg)
    return _set_shared_provider(chain_id, proxy)
