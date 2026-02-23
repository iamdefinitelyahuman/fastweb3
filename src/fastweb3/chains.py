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


POOL_EPOCH_SLEEP_S = 15 * 60
POOL_HEALTH_INTERVAL_S = 60.0
POOL_IMPROVE_FRACTION = 0.20
POOL_REPLACE_COOLDOWN_S = 10.0
POOL_EVICTION_COOLDOWN_S = 15 * 60

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
        yield  # pragma: no cover

    if deadline_s is None:
        deadline_s = max(PROBE_DEADLINE_MIN_S, PROBE_DEADLINE_MULTIPLIER * float(timeout_s))

    deadline = time.perf_counter() + float(deadline_s)

    work_q: "queue.Queue[str | None]" = queue.Queue()
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

    while True:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            break
        try:
            item = out_q.get(timeout=min(0.2, remaining))
        except queue.Empty:
            continue
        yield item


@dataclass
class MaintainerState:
    best_head: int | None = None
    rtt_by_url: dict[str, float] | None = None
    cooldown_until: dict[str, float] | None = None
    next_health_ts: float = 0.0
    next_replace_ts: float = 0.0

    def __post_init__(self) -> None:
        if self.rtt_by_url is None:
            self.rtt_by_url = {}
        if self.cooldown_until is None:
            self.cooldown_until = {}


def _maintainer_health_check(
    *,
    state: MaintainerState,
    provider: Provider,
    pinned: set[str],
    chain_id: int,
    max_lag_blocks: int,
    probe_timeout_s: float,
    now: float,
) -> None:
    if now < state.next_health_ts:
        return

    state.next_health_ts = now + POOL_HEALTH_INTERVAL_S

    for u in list(provider.urls()):
        if state.cooldown_until.get(u, 0.0) > now:
            continue

        try:
            pr = _probe_one(
                u,
                expected_chain_id=chain_id,
                timeout_s=min(1.0, probe_timeout_s),
            )

            state.rtt_by_url[u] = pr.rtt_ms
            state.best_head = pr.head if state.best_head is None else max(state.best_head, pr.head)

            if (
                state.best_head is not None
                and (state.best_head - pr.head) > max_lag_blocks
                and u not in pinned
            ):
                provider.remove_url(u)
                state.cooldown_until[u] = now + POOL_EVICTION_COOLDOWN_S

        except Exception:
            if u not in pinned:
                provider.remove_url(u)
                state.cooldown_until[u] = now + POOL_EVICTION_COOLDOWN_S


def _maintainer_handle_probe_result(
    *,
    state: MaintainerState,
    provider: Provider,
    pr: ProbeResult,
    pinned: set[str],
    target_pool: int,
    max_lag_blocks: int,
    now: float,
) -> None:
    if state.cooldown_until.get(pr.url, 0.0) > now:
        return

    state.best_head = pr.head if state.best_head is None else max(state.best_head, pr.head)

    if state.best_head is not None and (state.best_head - pr.head) > max_lag_blocks:
        return

    state.rtt_by_url[pr.url] = pr.rtt_ms

    desired_active = max(target_pool, len(pinned))

    if provider.endpoint_count() < desired_active:
        if pr.url not in provider.urls():
            provider.add_url(pr.url)
        return

    if now < state.next_replace_ts:
        return

    actives = [u for u in provider.urls() if u not in pinned]
    if not actives:
        return

    def rtt(u: str) -> float:
        return state.rtt_by_url.get(u, float("inf"))

    victim = max(actives, key=rtt)
    victim_rtt = rtt(victim)

    if pr.rtt_ms < victim_rtt * (1.0 - POOL_IMPROVE_FRACTION):
        provider.remove_url(victim)
        state.cooldown_until[victim] = now + POOL_EVICTION_COOLDOWN_S

        if pr.url not in provider.urls():
            provider.add_url(pr.url)

        state.next_replace_ts = now + POOL_REPLACE_COOLDOWN_S


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
    def maintain() -> None:
        state = MaintainerState(next_health_ts=time.time() + POOL_HEALTH_INTERVAL_S)

        while True:
            for pr in probe_urls_streaming(
                meta.rpc,
                expected_chain_id=chain_id,
                timeout_s=probe_timeout_s,
                max_workers=probe_workers,
            ):
                now = time.time()

                _maintainer_health_check(
                    state=state,
                    provider=provider,
                    pinned=pinned,
                    chain_id=chain_id,
                    max_lag_blocks=max_lag_blocks,
                    probe_timeout_s=probe_timeout_s,
                    now=now,
                )

                _maintainer_handle_probe_result(
                    state=state,
                    provider=provider,
                    pr=pr,
                    pinned=pinned,
                    target_pool=target_pool,
                    max_lag_blocks=max_lag_blocks,
                    now=now,
                )

            sleep_end = time.time() + POOL_EPOCH_SLEEP_S
            while time.time() < sleep_end:
                now = time.time()

                _maintainer_health_check(
                    state=state,
                    provider=provider,
                    pinned=pinned,
                    chain_id=chain_id,
                    max_lag_blocks=max_lag_blocks,
                    probe_timeout_s=probe_timeout_s,
                    now=now,
                )

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
    shared = _get_shared_provider(chain_id)
    if shared is not None:
        return shared

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

        if priority_urls:
            provider = Provider(
                priority_urls,
                retry_policy_read=retry_policy_read,
                retry_policy_write=retry_policy_write,
            )

            h.set_value(provider)

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

        deadline_s = max(
            PROBE_DEADLINE_MIN_S,
            PROBE_DEADLINE_MULTIPLIER * float(probe_timeout_s),
        )

        best_head: int | None = None

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

            h.set_value(provider)

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

    proxy = deferred_response(bg)
    return _set_shared_provider(chain_id, proxy)
