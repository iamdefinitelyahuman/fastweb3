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
from .utils import is_url_target, normalize_target, normalize_url

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
    """
    seen: set[str] = set()
    candidates: list[str] = []

    for u in urls:
        try:
            nt = normalize_target(u)
            if not is_url_target(nt):
                continue
            nu = normalize_url(nt)
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
    next_health_ts: float = 0.0
    next_replace_ts: float = 0.0


class PoolManager:
    """
    Per-chain pool intelligence.

    This object does not own threads. A single global scheduler thread calls
    `maintain()` periodically for every acquired pool manager.
    """

    def __init__(
        self,
        chain_id: int,
        *,
        target_pool: int = 6,
        max_lag_blocks: int = 8,
        probe_timeout_s: float = 1.5,
        probe_workers: int = 32,
        chains_registry: ChainsRegistry | None = None,
    ) -> None:
        self.chain_id = int(chain_id)
        self.target_pool = int(target_pool)
        self.max_lag_blocks = int(max_lag_blocks)
        self.probe_timeout_s = float(probe_timeout_s)
        self.probe_workers = int(probe_workers)

        self._reg = chains_registry or ChainsRegistry()
        self._meta: ChainMeta | None = None

        self._lock = threading.Lock()
        self._ready = threading.Event()  # latched once pool becomes non-empty

        self._active: list[str] = []
        self._active_set: set[str] = set()

        self._rtt_by_url: dict[str, float] = {}
        self._cooldown_until: dict[str, float] = {}
        self._state = _MaintainerState(next_health_ts=time.time() + POOL_HEALTH_INTERVAL_S)

        # Scheduler-driven: run an epoch immediately.
        self._next_epoch_ts: float = 0.0

    def best_urls(self, n: int, await_first: bool) -> list[str]:
        if n <= 0:
            return []

        with self._lock:
            if self._active:
                return list(self._active[:n])

        if await_first:
            self._ready.wait()

        with self._lock:
            return list(self._active[:n])

    def maintain(self, *, now: float | None = None) -> None:
        t = time.time() if now is None else float(now)

        if self._meta is None:
            self._meta = self._reg.get(self.chain_id)

        self._health_check(now=t)

        if t < self._next_epoch_ts:
            return

        for pr in probe_urls_streaming(
            self._meta.rpc,
            expected_chain_id=self.chain_id,
            timeout_s=self.probe_timeout_s,
            max_workers=self.probe_workers,
        ):
            now2 = time.time()
            self._health_check(now=now2)
            self._handle_probe_result(pr=pr, now=now2)

        self._ready.set()
        self._next_epoch_ts = time.time() + POOL_EPOCH_SLEEP_S

    # --- internals ---

    def _promote_active(self, url: str) -> None:
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
        with self._lock:
            self._active.sort(key=lambda u: self._rtt_by_url.get(u, float("inf")))

    def _health_check(self, *, now: float) -> None:
        st = self._state
        if now < st.next_health_ts:
            return
        st.next_health_ts = now + POOL_HEALTH_INTERVAL_S

        with self._lock:
            active = list(self._active)

        best_head = st.best_head

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
                best_head = pr.head if best_head is None else max(best_head, pr.head)

                if best_head is not None and (best_head - pr.head) > self.max_lag_blocks:
                    self._remove_active(u)
                    self._cooldown_until[u] = now + POOL_EVICTION_COOLDOWN_S
            except Exception:
                self._remove_active(u)
                self._cooldown_until[u] = now + POOL_EVICTION_COOLDOWN_S

        st.best_head = best_head
        self._sort_active_by_rtt()

    def _handle_probe_result(self, *, pr: ProbeResult, now: float) -> None:
        if self._cooldown_until.get(pr.url, 0.0) > now:
            return

        st = self._state
        st.best_head = pr.head if st.best_head is None else max(st.best_head, pr.head)

        if st.best_head is not None and (st.best_head - pr.head) > self.max_lag_blocks:
            return

        self._rtt_by_url[pr.url] = pr.rtt_ms

        with self._lock:
            active_count = len(self._active)
            is_active = pr.url in self._active_set
            active_urls = list(self._active)

        if is_active:
            self._sort_active_by_rtt()
            return

        if active_count < self.target_pool:
            self._promote_active(pr.url)
            self._sort_active_by_rtt()
            return

        if now < st.next_replace_ts:
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

            st.next_replace_ts = now + POOL_REPLACE_COOLDOWN_S


# --- global scheduler + registry ---


class _Scheduler:
    def __init__(self):
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._thread = None

    def ensure_running(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and not self._event.is_set():
                return
            if self._thread is not None:
                self._thread.join(timeout=1.0)
            self._event = threading.Event()
            self._thread = threading.Thread(target=self._loop, args=(self._event,), daemon=True)
            self._thread.start()

    def stop_if_idle(self):
        with _pool_lock:
            any_active = any(v > 0 for v in _pool_refcount.values())
        if any_active:
            return
        with self._lock:
            self._event.set()

    def _loop(self, event):
        while not event.is_set():
            with _pool_lock:
                managers = list(_pool_by_chain.values())

            now = time.time()
            for pm in managers:
                if event.is_set():
                    break
                try:
                    pm.maintain(now=now)
                except Exception:
                    continue

            event.wait(timeout=60)


_pool_lock = threading.Lock()
_pool_by_chain: dict[int, PoolManager] = {}
_pool_refcount: dict[int, int] = {}
_scheduler = _Scheduler()


def acquire_pool_manager(
    chain_id: int,
    *,
    target_pool: int = 6,
    max_lag_blocks: int = 8,
    probe_timeout_s: float = 1.5,
    probe_workers: int = 32,
) -> PoolManager:
    cid = int(chain_id)
    with _pool_lock:
        pm = _pool_by_chain.get(cid)
        if pm is None:
            pm = PoolManager(
                cid,
                target_pool=target_pool,
                max_lag_blocks=max_lag_blocks,
                probe_timeout_s=probe_timeout_s,
                probe_workers=probe_workers,
            )
            _pool_by_chain[cid] = pm

        _pool_refcount[cid] = _pool_refcount.get(cid, 0) + 1
        _scheduler.ensure_running()
        return pm


def release_pool_manager(chain_id: int) -> None:
    cid = int(chain_id)
    with _pool_lock:
        cur = _pool_refcount.get(cid, 0)
        if cur <= 0:
            return
        _pool_refcount[cid] = cur - 1

    _scheduler.stop_if_idle()
