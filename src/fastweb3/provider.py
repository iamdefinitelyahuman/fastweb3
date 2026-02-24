from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from .endpoint import Endpoint, Formatter
from .errors import AllEndpointsFailed, NoEndpoints, NoPrimaryEndpoint, RPCError, TransportError
from .rpc_pool import PoolManager
from .utils import normalize_url


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.05
    retry_on_rpc_error: bool = False


def _default_is_retryable_exc(exc: Exception) -> bool:
    return isinstance(exc, TransportError)


@dataclass
class _EndpointState:
    cooldown_until: float = 0.0
    failures: int = 0


class Provider:
    """
    Routes requests across:
      - a primary endpoint (explicit, stable identity)
      - an internal pool (per-instance, user-supplied, permanent)
      - a shared PoolManager (per-chain public pool intelligence)

    Pool routing:
      candidates = internal_pool + pool_manager.best_urls(needed)
      where needed fills up to desired_pool_size (option B)
    """

    def __init__(
        self,
        internal_urls: list[str] | None = None,
        *,
        pool_manager: PoolManager | None = None,
        desired_pool_size: int = 6,
        retry_policy_pool: RetryPolicy | None = None,
        is_retryable_exc: Callable[[Exception], bool] = _default_is_retryable_exc,
    ) -> None:
        self._lock = threading.Lock()
        self._tls = threading.local()
        self._rr = 0

        self.pool_manager = pool_manager
        self.desired_pool_size = max(0, int(desired_pool_size))
        self.retry_policy_pool = retry_policy_pool or RetryPolicy(max_attempts=3)
        self.is_retryable_exc = is_retryable_exc

        # Internal pool URLs (normalized, insertion-ordered) + membership set.
        self._internal_urls: list[str] = []
        self._internal_seen: set[str] = set()

        # Endpoint cache for any URL we might touch (internal or manager).
        self._eps_by_url: dict[str, Endpoint] = {}
        self._state: dict[Endpoint, _EndpointState] = {}

        # Primary is a specific Endpoint instance (stable identity), or None.
        self._primary: Endpoint | None = None

        for u in internal_urls or []:
            self.add_url(u, priority=False)

    # ----------------------------
    # endpoint cache helpers
    # ----------------------------

    def _get_or_create_endpoint(self, url: str) -> Endpoint:
        nu = normalize_url(url)
        with self._lock:
            ep = self._eps_by_url.get(nu)
            if ep is not None:
                return ep
            ep = Endpoint(nu)
            self._eps_by_url[nu] = ep
            self._state[ep] = _EndpointState()
            return ep

    # ----------------------------
    # primary management
    # ----------------------------

    def set_primary(self, url: str) -> None:
        """
        Set the primary endpoint to `url`.

        Note: this does NOT add the URL to the internal pool.
        (Web3 can choose to do that in primary-only mode.)
        """
        ep = self._get_or_create_endpoint(url)
        with self._lock:
            self._primary = ep

    def clear_primary(self) -> None:
        with self._lock:
            self._primary = None

    def primary_url(self) -> str | None:
        with self._lock:
            return self._primary.url if self._primary is not None else None

    def has_primary(self) -> bool:
        with self._lock:
            return self._primary is not None

    def _get_primary(self) -> Endpoint:
        with self._lock:
            if self._primary is None:
                raise NoPrimaryEndpoint("Primary endpoint is unset")
            return self._primary

    # ----------------------------
    # internal pool management
    # ----------------------------

    def add_url(self, url: str, *, priority: bool = False) -> None:
        """
        Add a URL to the internal pool (per-instance, permanent).
        Deduped by normalized URL.
        """
        nu = normalize_url(url)
        with self._lock:
            if nu in self._internal_seen:
                return
            self._internal_seen.add(nu)
            if priority:
                self._internal_urls.insert(0, nu)
            else:
                self._internal_urls.append(nu)

        # Ensure Endpoint exists in cache (outside lock is fine but keep simple)
        self._get_or_create_endpoint(nu)

    def remove_url(self, url: str) -> None:
        """
        Remove a URL from the internal pool. No-op if missing.

        Does not delete the Endpoint from cache (it may be referenced by primary or manager).
        If the removed URL is the primary, primary is cleared.
        """
        nu = normalize_url(url)
        with self._lock:
            if nu not in self._internal_seen:
                return
            self._internal_seen.remove(nu)
            try:
                self._internal_urls.remove(nu)
            except ValueError:
                pass

            # If primary points at this endpoint, clear it.
            ep = self._eps_by_url.get(nu)
            if ep is not None and self._primary is ep:
                self._primary = None

    def urls(self) -> list[str]:
        """Return internal pool URLs (snapshot)."""
        with self._lock:
            return list(self._internal_urls)

    def endpoint_count(self) -> int:
        """Count of internal pool endpoints (not including manager candidates)."""
        with self._lock:
            return len(self._internal_urls)

    def close(self) -> None:
        """
        Close all cached Endpoint transports (internal + manager + primary).
        """
        with self._lock:
            eps = list(self._eps_by_url.values())
            self._eps_by_url.clear()
            self._state.clear()
            self._internal_urls.clear()
            self._internal_seen.clear()
            self._primary = None

        for ep in eps:
            ep.close()

    # ----------------------------
    # cooldown/backoff
    # ----------------------------

    def _cooldown_seconds(self, exc: Exception, failures: int) -> float:
        """
        Decide how long to avoid an endpoint after a failure.

        - Transport errors get exponential-ish backoff (capped).
        - HTTP 429 gets a longer cooldown (also capped).
        """
        base = 0.25 * (2 ** min(max(failures, 1) - 1, 6))
        base_cap = 30.0

        status = getattr(exc, "status_code", None) if isinstance(exc, TransportError) else None
        if status == 429:
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

    def _eligible_endpoints(self, eps: list[Endpoint]) -> list[Endpoint]:
        """
        Filter endpoints that are not currently in cooldown.
        If all are in cooldown, return the full list (try anyway).
        """
        now = time.time()
        with self._lock:
            eligible = [
                ep for ep in eps if self._state.get(ep, _EndpointState()).cooldown_until <= now
            ]
        return eligible if eligible else eps

    # ----------------------------
    # candidate building (pool route)
    # ----------------------------

    def _pool_candidates(self) -> list[Endpoint]:
        """
        Build the candidate endpoint list for pool routing:
          internal_pool + pool_manager.best_urls(needed)
        Deduped and ordered (internal first, then manager).
        """
        with self._lock:
            internal = list(self._internal_urls)

        needed = max(0, self.desired_pool_size - len(internal))
        manager_urls: list[str] = []
        if needed > 0 and self.pool_manager is not None:
            manager_urls = self.pool_manager.best_urls(needed)

        # Dedup preserve order by normalized URL
        seen: set[str] = set()
        merged: list[str] = []

        for u in internal + manager_urls:
            nu = normalize_url(u)
            if nu in seen:
                continue
            seen.add(nu)
            merged.append(nu)

        if not merged:
            raise NoEndpoints("No endpoints available")

        return [self._get_or_create_endpoint(u) for u in merged]

    # ----------------------------
    # pinning
    # ----------------------------

    def _get_pinned(self) -> Optional[Endpoint]:
        return getattr(self._tls, "pinned", None)

    @contextmanager
    def pin(self, *, route: str = "pool") -> Iterator[Endpoint]:
        """
        Pin this thread to a single endpoint for deterministic routing.

        route="pool": pins to the current RR choice from the merged pool candidates.
        route="primary": pins to primary (raises if unset).
        """
        if route not in ("pool", "primary"):
            raise ValueError("route must be 'pool' or 'primary'")

        prev = self._get_pinned()

        if route == "primary":
            chosen = self._get_primary()
        else:
            eps = self._eligible_endpoints(self._pool_candidates())
            with self._lock:
                start = self._rr % len(eps)
                self._rr = (self._rr + 1) % (1 << 30)
            chosen = eps[start]

        self._tls.pinned = chosen
        try:
            yield chosen
        finally:
            self._tls.pinned = prev

    # ----------------------------
    # request routing
    # ----------------------------

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        route: str = "pool",
        formatter: Formatter | None = None,
    ) -> Any:
        """
        Perform a JSON-RPC request routed either to the merged pool or to the primary.

        - route="pool": RR+retry across (internal_pool + pool_manager best URLs)
        - route="primary": primary-only (raises if primary unset)
        """
        if route not in ("pool", "primary"):
            raise ValueError("route must be 'pool' or 'primary'")

        pinned = self._get_pinned()
        if pinned is not None:
            return pinned.request(method, params, formatter=formatter)

        if route == "primary":
            ep = self._get_primary()
            try:
                result = ep.request(method, params, formatter=formatter)
                self._mark_success(ep)
                return result
            except Exception as exc:
                if self.is_retryable_exc(exc):
                    self._mark_failure(ep, exc)
                raise AllEndpointsFailed(exc) from exc

        # route == "pool"
        candidates = self._pool_candidates()
        eps = self._eligible_endpoints(candidates)

        policy = self.retry_policy_pool
        attempts = min(max(1, policy.max_attempts), len(eps))
        last_exc: Exception | None = None

        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)

        for i in range(attempts):
            ep = eps[(start + i) % len(eps)]
            try:
                result = ep.request(method, params, formatter=formatter)
                self._mark_success(ep)
                return result

            except RPCError as exc:
                last_exc = exc
                if policy.retry_on_rpc_error and i < attempts - 1:
                    time.sleep(policy.backoff_seconds)
                    continue
                raise

            except Exception as exc:
                last_exc = exc
                if self.is_retryable_exc(exc):
                    self._mark_failure(ep, exc)
                    if i < attempts - 1:
                        time.sleep(policy.backoff_seconds)
                        continue
                break

        raise AllEndpointsFailed(last_exc)
