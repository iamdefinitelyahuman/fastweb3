# src/fastweb3/rpc_pool.py
from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass
from typing import Iterator

import httpx

from .transport import HTTPTransportConfig, WSSTransportConfig, make_transport
from .utils import normalize_url

CHAINS_URL = (
    "https://raw.githubusercontent.com/ethereum-lists/chains/master/_data/chains/"
    "eip155-{chain_id}.json"
)

# --- pool tuning ---
POOL_EPOCH_SLEEP_S = 15 * 60
POOL_HEALTH_INTERVAL_S = 60.0
POOL_IMPROVE_FRACTION = 0.20
POOL_REPLACE_COOLDOWN_S = 10.0
POOL_EVICTION_COOLDOWN_S = 15 * 60

PROBE_DEADLINE_MULTIPLIER = 4.0
PROBE_DEADLINE_MIN_S = 5.0


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


def _has_wss_support() -> bool:
    """
    WebSocket support is optional. If websocket-client isn't installed,
    we treat ws/wss URLs as not probeable to avoid exception hot paths.
    """
    try:
        import fastweb3.transport.ws as ws_mod
    except Exception:
        return False
    return ws_mod.websocket is not None


def _is_probeable(url: str) -> bool:
    u = url.lower()
    if u.startswith(("http://", "https://")):
        return True
    if u.startswith(("ws://", "wss://")):
        return _has_wss_support()
    return False


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

    http_cfg = HTTPTransportConfig(
        timeout=timeout_s,
        connect_timeout=timeout_s,
        read_timeout=timeout_s,
        write_timeout=timeout_s,
        pool_timeout=timeout_s,
        max_connections=10,
        max_keepalive_connections=5,
        keepalive_expiry=10.0,
    )
    wss_cfg = WSSTransportConfig(
        connect_timeout=timeout_s,
        recv_timeout=timeout_s,
    )

    t0 = time.perf_counter()
    tr = make_transport(url, http=http_cfg, wss=wss_cfg)
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
        try:
            nu = normalize_url(u)
        except Exception:
            continue
        if not _is_probeable(nu):
            continue
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

    http_cfg = HTTPTransportConfig(
        timeout=timeout_s,
        connect_timeout=timeout_s,
        read_timeout=timeout_s,
        write_timeout=timeout_s,
        pool_timeout=timeout_s,
        max_connections=max(50, worker_count),
        max_keepalive_connections=min(20, worker_count),
        keepalive_expiry=10.0,
    )
    wss_cfg = WSSTransportConfig(
        connect_timeout=timeout_s,
        recv_timeout=timeout_s,
    )

    def worker() -> None:
        while True:
            u = work_q.get()
            if u is None:
                return

            t0 = time.perf_counter()
            tr = make_transport(u, http=http_cfg, wss=wss_cfg)
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
class _MaintainerState:
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


class PoolManager:
    """
    Shared per-chain RPC pool intelligence.

    - Maintains an *active* best-first list of URLs for a given chain_id.
    - Only active URLs are returned to callers.
    - If best_urls(n) is called before any active URL exists, it blocks until
      the pool becomes non-empty at least once.

    Notes:
      - Active membership and ordering is managed by a background maintainer thread.
      - The pool manager returns URLs only; each Provider maintains its own Endpoint objects
        and cooldown state.
    """

    def __init__(
        self,
        chain_id: int,
        *,
        target_pool: int = 6,
        max_lag_blocks: int = 8,
        probe_timeout_s: float = 1.5,
        probe_workers: int = 32,
    ) -> None:
        self.chain_id = int(chain_id)
        self.target_pool = int(target_pool)
        self.max_lag_blocks = int(max_lag_blocks)
        self.probe_timeout_s = float(probe_timeout_s)
        self.probe_workers = int(probe_workers)

        self._lock = threading.Lock()
        self._ready = threading.Event()  # latched once pool becomes non-empty

        # Active set best-first, plus quick membership.
        self._active: list[str] = []
        self._active_set: set[str] = set()

        # Perf/health data for scoring.
        self._best_head: int | None = None
        self._rtt_by_url: dict[str, float] = {}
        self._cooldown_until: dict[str, float] = {}

        # Start the maintainer thread immediately.
        threading.Thread(target=self._maintain, daemon=True).start()

    def best_urls(self, n: int, await_first: bool) -> list[str]:
        """
        Return up to n URLs from the active set, best-first.

        If the active set is empty, block until the pool becomes non-empty
        at least once.
        """
        if n <= 0:
            return []

        with self._lock:
            if self._active:
                return list(self._active[:n])

        # No active URLs yet: block until at least one is available.
        if await_first:
            self._ready.wait()

        with self._lock:
            return list(self._active[:n])

    # --- internal maintainer ---

    def _promote_active(self, url: str) -> None:
        """Ensure url is in active set (best-first ordering handled elsewhere)."""
        with self._lock:
            if url in self._active_set:
                return
            self._active.append(url)
            self._active_set.add(url)
            if len(self._active) == 1:
                self._ready.set()

    def _remove_active(self, url: str) -> None:
        with self._lock:
            if url not in self._active_set:
                return
            self._active_set.remove(url)
            try:
                self._active.remove(url)
            except ValueError:
                pass

    def _sort_active_by_rtt(self) -> None:
        # best-first = lowest RTT
        with self._lock:
            self._active.sort(key=lambda u: self._rtt_by_url.get(u, float("inf")))

    def _health_check(self, *, state: _MaintainerState, meta: ChainMeta, now: float) -> None:
        if now < state.next_health_ts:
            return
        state.next_health_ts = now + POOL_HEALTH_INTERVAL_S

        # Snapshot active list to probe without holding lock during network I/O.
        with self._lock:
            active = list(self._active)

        for u in active:
            if self._cooldown_until.get(u, 0.0) > now:
                continue
            try:
                pr = _probe_one(
                    u,
                    expected_chain_id=self.chain_id,
                    timeout_s=min(1.0, self.probe_timeout_s),
                )
                self._rtt_by_url[u] = pr.rtt_ms
                self._best_head = (
                    pr.head if self._best_head is None else max(self._best_head, pr.head)
                )

                if (
                    self._best_head is not None
                    and (self._best_head - pr.head) > self.max_lag_blocks
                ):
                    self._remove_active(u)
                    self._cooldown_until[u] = now + POOL_EVICTION_COOLDOWN_S
            except Exception:
                self._remove_active(u)
                self._cooldown_until[u] = now + POOL_EVICTION_COOLDOWN_S

        self._sort_active_by_rtt()

    def _handle_probe_result(self, *, state: _MaintainerState, pr: ProbeResult, now: float) -> None:
        if self._cooldown_until.get(pr.url, 0.0) > now:
            return

        self._best_head = pr.head if self._best_head is None else max(self._best_head, pr.head)

        if self._best_head is not None and (self._best_head - pr.head) > self.max_lag_blocks:
            return

        self._rtt_by_url[pr.url] = pr.rtt_ms

        with self._lock:
            active_count = len(self._active)
            active_set = set(self._active_set)
            active_urls = list(self._active)

        if pr.url in active_set:
            # Update RTT and re-sort
            self._sort_active_by_rtt()
            return

        # If we have room, just promote.
        if active_count < self.target_pool:
            self._promote_active(pr.url)
            self._sort_active_by_rtt()
            return

        # Replace worst active if significantly better and not too soon.
        if now < state.next_replace_ts:
            return

        def rtt(u: str) -> float:
            return self._rtt_by_url.get(u, float("inf"))

        victim = max(active_urls, key=rtt, default=None)
        if victim is None:
            return

        victim_rtt = rtt(victim)
        if pr.rtt_ms < victim_rtt * (1.0 - POOL_IMPROVE_FRACTION):
            self._remove_active(victim)
            self._cooldown_until[victim] = now + POOL_EVICTION_COOLDOWN_S

            self._promote_active(pr.url)
            self._sort_active_by_rtt()

            state.next_replace_ts = now + POOL_REPLACE_COOLDOWN_S

    def _maintain(self) -> None:
        reg = ChainsRegistry()
        meta = reg.get(self.chain_id)

        state = _MaintainerState(next_health_ts=time.time() + POOL_HEALTH_INTERVAL_S)

        while True:
            for pr in probe_urls_streaming(
                meta.rpc,
                expected_chain_id=self.chain_id,
                timeout_s=self.probe_timeout_s,
                max_workers=self.probe_workers,
            ):
                now = time.time()
                self._health_check(state=state, meta=meta, now=now)
                self._handle_probe_result(state=state, pr=pr, now=now)

            self._ready.set()

            sleep_end = time.time() + POOL_EPOCH_SLEEP_S
            while time.time() < sleep_end:
                now = time.time()
                self._health_check(state=state, meta=meta, now=now)
                time.sleep(5)


# --- singleton manager registry ---

_pool_lock = threading.Lock()
_pool_by_chain: dict[int, PoolManager] = {}


def get_pool_manager(
    chain_id: int,
    *,
    target_pool: int = 6,
    max_lag_blocks: int = 8,
    probe_timeout_s: float = 1.5,
    probe_workers: int = 32,
) -> PoolManager:
    """
    Get (or create) a shared PoolManager for chain_id.
    Starts the background maintainer once per chain.
    """
    cid = int(chain_id)
    with _pool_lock:
        pm = _pool_by_chain.get(cid)
        if pm is not None:
            return pm
        pm = PoolManager(
            cid,
            target_pool=target_pool,
            max_lag_blocks=max_lag_blocks,
            probe_timeout_s=probe_timeout_s,
            probe_workers=probe_workers,
        )
        _pool_by_chain[cid] = pm
        return pm
