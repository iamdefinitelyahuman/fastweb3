from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from .endpoint import Endpoint, Formatter
from .errors import AllEndpointsFailed, NoEndpoints, RPCError, TransportError
from .transport import Transport
from .utils import normalize_url


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.05
    retry_on_rpc_error: bool = False


def _default_is_retryable_exc(exc: Exception) -> bool:
    return isinstance(exc, TransportError)


TransportFactory = Callable[[str], Transport]


@dataclass
class _EndpointState:
    cooldown_until: float = 0.0
    failures: int = 0


class Provider:
    """
    Routes requests across multiple endpoints.

    v0:
      - reads: round-robin + retry/failover
      - writes: primary only
    Scaffolding:
      - per-thread pinning context (for future batching determinism)

    Notes:
      - supports dynamic pool growth via add_url()
      - dedups URLs using a normalized form
      - cooldown/backoff: temporarily avoid endpoints that are failing (esp. 429s)
    """

    def __init__(
        self,
        urls: list[str] | None = None,
        *,
        retry_policy_read: RetryPolicy | None = None,
        retry_policy_write: RetryPolicy | None = None,
        is_retryable_exc: Callable[[Exception], bool] = _default_is_retryable_exc,
    ) -> None:
        self._lock = threading.Lock()

        self._endpoints: list[Endpoint] = []
        self._seen_urls: set[str] = set()
        self._state: dict[Endpoint, _EndpointState] = {}

        if urls:
            # Reuse add_url logic so normalization + dedup is identical.
            for u in urls:
                self.add_url(u, priority=False)

        self._rr = 0
        self._tls = threading.local()

        self.retry_policy_read = retry_policy_read or RetryPolicy(max_attempts=3)
        self.retry_policy_write = retry_policy_write or RetryPolicy(max_attempts=1)
        self.is_retryable_exc = is_retryable_exc

    def _cooldown_seconds(self, exc: Exception, failures: int) -> float:
        """
        Decide how long to avoid an endpoint after a failure.

        - Transport errors get exponential-ish backoff (capped).
        - HTTP 429 gets a longer cooldown (also capped).
        """
        # Base exponential backoff (fast growth but capped)
        # failures=1 -> 0.25s, 2 -> 0.5s, 3 -> 1s, 4 -> 2s, ...
        base = 0.25 * (2 ** min(max(failures, 1) - 1, 6))
        base_cap = 30.0

        status = getattr(exc, "status_code", None) if isinstance(exc, TransportError) else None
        if status == 429:
            # Rate limiting: longer cooldown; keep minimum non-trivial.
            return min(60.0, max(5.0, base * 4))

        return min(base_cap, base)

    def _mark_failure(self, ep: Endpoint, exc: Exception) -> None:
        now = time.time()
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                st = _EndpointState()
                self._state[ep] = st
            st.failures += 1
            st.cooldown_until = now + self._cooldown_seconds(exc, st.failures)

    def _mark_success(self, ep: Endpoint) -> None:
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                st = _EndpointState()
                self._state[ep] = st
            st.failures = 0
            st.cooldown_until = 0.0

    def _eligible_snapshot(self) -> list[Endpoint]:
        """
        Return endpoints that are not currently in cooldown.
        If all endpoints are in cooldown, returns the full snapshot (try anyway).
        """
        now = time.time()
        with self._lock:
            eps = list(self._endpoints)
            eligible = [
                ep for ep in eps if self._state.get(ep, _EndpointState()).cooldown_until <= now
            ]
        return eligible if eligible else eps

    def add_url(self, url: str, *, priority: bool = False) -> None:
        """
        Add a new endpoint URL to the provider pool (thread-safe).

        If the (normalized) URL already exists, this is a no-op.
        """
        nu = normalize_url(url)

        with self._lock:
            if nu in self._seen_urls:
                return
            self._seen_urls.add(nu)

            ep = Endpoint(nu)
            self._state[ep] = _EndpointState()

            if priority:
                self._endpoints.insert(0, ep)
            else:
                self._endpoints.append(ep)

    def urls(self) -> list[str]:
        """
        Return current endpoint URLs in provider order (thread-safe snapshot).
        """
        with self._lock:
            return [e.url for e in self._endpoints]

    def remove_url(self, url: str) -> None:
        """
        Remove an endpoint URL from the provider pool (thread-safe). No-op if missing.
        """
        nu = normalize_url(url)
        ep_to_close: Endpoint | None = None

        with self._lock:
            for i, ep in enumerate(self._endpoints):
                if normalize_url(ep.url) == nu:
                    ep_to_close = ep
                    del self._endpoints[i]
                    self._seen_urls.discard(nu)
                    self._state.pop(ep, None)
                    break

        if ep_to_close is not None:
            ep_to_close.close()

    def endpoint_count(self) -> int:
        with self._lock:
            return len(self._endpoints)

    def close(self) -> None:
        # Close a snapshot to avoid holding the lock while closing transports.
        with self._lock:
            eps = list(self._endpoints)
        for e in eps:
            e.close()

    def _snapshot(self) -> list[Endpoint]:
        with self._lock:
            return list(self._endpoints)

    def _primary(self) -> Endpoint:
        with self._lock:
            if not self._endpoints:
                raise NoEndpoints("No endpoints available")
            return self._endpoints[0]

    def _pick_rr_from(self, eps: list[Endpoint]) -> Endpoint:
        # Caller provides a snapshot 'eps' (non-empty).
        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)
        return eps[start]

    def _get_pinned(self) -> Optional[Endpoint]:
        return getattr(self._tls, "pinned", None)

    @contextmanager
    def pin(self, *, kind: str = "read") -> Iterator[Endpoint]:
        """
        Pin this thread to a single endpoint for deterministic routing.
        Intended for future batching contexts.

        kind="read" pins to the current RR endpoint snapshot.
        kind="write" pins to primary (conservative).

        Note: If no endpoints exist yet, this raises NoEndpoints.
        """
        if kind not in ("read", "write"):
            raise ValueError("kind must be 'read' or 'write'")

        prev = self._get_pinned()
        if kind == "write":
            chosen = self._primary()
        else:
            eps = self._eligible_snapshot()
            if not eps:
                raise NoEndpoints("No endpoints available")
            chosen = self._pick_rr_from(eps)

        self._tls.pinned = chosen
        try:
            yield chosen
        finally:
            self._tls.pinned = prev

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        kind: str = "read",
        formatter: Formatter | None = None,
    ) -> Any:
        if kind not in ("read", "write"):
            raise ValueError("kind must be 'read' or 'write'")

        pinned = self._get_pinned()
        if pinned is not None:
            return pinned.request(method, params, formatter=formatter)

        if kind == "write":
            ep = self._primary()
            try:
                result = ep.request(method, params, formatter=formatter)
                self._mark_success(ep)
                return result
            except Exception as exc:
                if self.is_retryable_exc(exc):
                    self._mark_failure(ep, exc)
                raise AllEndpointsFailed(exc) from exc

        eps = self._eligible_snapshot()
        if not eps:
            raise NoEndpoints("No endpoints available")

        policy = self.retry_policy_read
        attempts = min(max(1, policy.max_attempts), len(eps))
        last_exc: Exception | None = None

        for attempt in range(1, attempts + 1):
            with self._lock:
                start = self._rr % len(eps)
                self._rr = (self._rr + 1) % (1 << 30)
            ep = eps[(start + (attempt - 1)) % len(eps)]

            try:
                result = ep.request(method, params, formatter=formatter)
                self._mark_success(ep)
                return result

            except RPCError as exc:
                last_exc = exc
                if policy.retry_on_rpc_error and attempt < attempts:
                    time.sleep(policy.backoff_seconds)
                    continue
                raise

            except Exception as exc:
                last_exc = exc
                if self.is_retryable_exc(exc):
                    self._mark_failure(ep, exc)
                    if attempt < attempts:
                        time.sleep(policy.backoff_seconds)
                        continue
                break

        raise AllEndpointsFailed(last_exc)
