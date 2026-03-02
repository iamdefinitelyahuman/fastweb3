# src/fastweb3/provider.py
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from .endpoint import Endpoint, Formatter
from .errors import (
    AllEndpointsFailed,
    NoEndpoints,
    NoPrimaryEndpoint,
    RPCError,
    TransportError,
)
from .formatters import to_int
from .rpc_pool import PoolManager
from .utils import normalize_url


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.05


@dataclass(frozen=True)
class _BatchCall:
    method: str
    params: list[Any] | tuple[Any, ...]
    formatter: Formatter | None = None
    freshness: Callable[[Any, int, int], bool] | None = None


def _default_is_retryable_exc(exc: Exception) -> bool:
    return isinstance(exc, TransportError)


@dataclass
class _EndpointState:
    # Cooldown due to request/transport failures (exponential-ish backoff)
    error_cooldown_until: float = 0.0
    failures: int = 0

    # Cooldown due to stale tip observation
    tip_cooldown_until: float = 0.0

    # Last tip observed from this endpoint URL (LB backends may vary)
    last_tip: int | None = None


class _FreshnessUnmet(TransportError):
    """Internal sentinel for 'response received, but endpoint tip is too stale for this call'."""


class Provider:
    """
    Routes requests across:
      - a primary endpoint (explicit, stable identity)
      - an internal pool (per-instance, user-supplied, permanent)
      - a shared PoolManager (per-chain public pool intelligence)

    Pool routing:
      candidates = internal_pool + pool_manager.best_urls(needed)
      where needed fills up to desired_pool_size (option B)

    Tip tracking:
      - Every pooled request is executed as a batch: [eth_blockNumber, userCall]
      - Track the best (highest) tip observed across any endpoint
      - Endpoints that report a tip lower than best are temporarily demoted (cooldown)
      - Track per-endpoint last_tip (for preferential retries)

    Freshness enforcement (optional):
      - request() accepts `freshness(response, required_tip, returned_tip) -> bool`
      - required_tip is snapshotted at the start of each attempt (concurrency-safe)
      - If freshness rejects, rotate endpoints preferring highest last_tip, backoff briefly, retry
    """

    # Bound how long we spin waiting for an endpoint to satisfy strict freshness.
    _FRESHNESS_WAIT_CAP_SECONDS: float = 5.0

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
        self._rr = 0

        if pool_manager is None:
            desired_pool_size = 0
        self.desired_pool_size = max(
            len(internal_urls or []),
            max(0, int(desired_pool_size)),
        )

        self.pool_manager = pool_manager
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

        # Best (highest) chain tip we've observed across any endpoint.
        self._best_tip: int | None = None

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
            return self._primary.url if self._primary is not None else None  # type: ignore[attr-defined]

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
            self._best_tip = None

        for ep in eps:
            ep.close()

    # ----------------------------
    # cooldown/backoff (errors)
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
            st.error_cooldown_until = now + self._cooldown_seconds(exc, st.failures)

    def _mark_success(self, ep: Endpoint) -> None:
        # Reset *error* failures/cooldown. Do NOT clear tip demotion.
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                st = _EndpointState()
                self._state[ep] = st
            st.failures = 0
            st.error_cooldown_until = 0.0

    # ----------------------------
    # tip tracking / demotion
    # ----------------------------

    def _tip_cooldown_seconds(self, lag_blocks: int) -> float:
        """
        Cooldown for endpoints that report a stale tip vs best known.

        Simple, bounded "temporary demotion", not a ban.
        """
        return min(30.0, 0.5 + 0.25 * max(0, int(lag_blocks)))

    def _update_tip_and_maybe_demote(self, ep: Endpoint, returned_tip: int) -> None:
        now = time.time()
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                st = _EndpointState()
                self._state[ep] = st

            st.last_tip = int(returned_tip)

            best = self._best_tip
            if best is None or returned_tip > best:
                self._best_tip = int(returned_tip)
                return

            if returned_tip < best:
                lag = best - returned_tip
                st.tip_cooldown_until = max(
                    st.tip_cooldown_until, now + self._tip_cooldown_seconds(lag)
                )

    def _best_tip_snapshot(self) -> int:
        # Used as required_tip at attempt start (concurrency-safe monotonic guarantee).
        with self._lock:
            return int(self._best_tip or 0)

    def _last_tip(self, ep: Endpoint) -> int:
        with self._lock:
            st = self._state.get(ep)
            if st is None or st.last_tip is None:
                return -1
            return int(st.last_tip)

    # ----------------------------
    # eligibility
    # ----------------------------

    def _eligible_endpoints(self, eps: list[Endpoint]) -> list[Endpoint]:
        """
        Filter endpoints that are not currently in cooldown.
        If all are in cooldown, return the full list (try anyway).
        """
        now = time.time()
        with self._lock:
            eligible: list[Endpoint] = []
            for ep in eps:
                st = self._state.get(ep, _EndpointState())
                cooldown_until = max(st.error_cooldown_until, st.tip_cooldown_until)
                if cooldown_until <= now:
                    eligible.append(ep)

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
            await_first = not (internal or self._primary)
            manager_urls = self.pool_manager.best_urls(needed, await_first)

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
            if self._primary is not None:
                return [self._primary]
            raise NoEndpoints("No endpoints available")

        return [self._get_or_create_endpoint(u) for u in merged]

    # ----------------------------
    # attempt (single call)
    # ----------------------------

    def _attempt(
        self,
        ep: Endpoint,
        method: str,
        params: list[Any] | tuple[Any, ...],
        formatter: Formatter | None,
        freshness: Callable[[Any, int, int], bool] | None,
    ) -> tuple[Any | None, Exception | None]:
        """
        Attempt the call once on `ep`.

        Returns: (value, exc)
          - value is non-None on success
          - exc is non-None on retryable failure or freshness rejection
          - raises non-retryable errors, including RPCError (always bubbles)

        Notes:
          - Freshness rejection returns _FreshnessUnmet (do NOT mark_failure).
          - Transport failures (is_retryable_exc) mark_failure.
          - required_tip is snapshotted at attempt start (concurrency-safe).
        """
        required_tip = self._best_tip_snapshot()

        try:
            if self.desired_pool_size == 0:
                # No tip probing in primary-only mode.
                result = ep.request(method, params, formatter)
                self._mark_success(ep)
                return result, None

            tip, result = ep.request_batch(
                ("eth_blockNumber", (), to_int),
                (method, params, formatter),
            )
            for item in (result, tip):
                if isinstance(item, RPCError):
                    raise item

            self._update_tip_and_maybe_demote(ep, tip)
            self._mark_success(ep)

            if freshness is None:
                return result, None

            if freshness(result, required_tip, tip):
                return result, None

            return None, _FreshnessUnmet("Freshness unmet")

        except RPCError:
            # Always bubble RPCError immediately.
            raise

        except Exception as exc:
            if self.is_retryable_exc(exc):
                self._mark_failure(ep, exc)
                return None, exc
            raise

    # ----------------------------
    # attempt (batch)
    # ----------------------------

    def _attempt_batch(
        self,
        ep: Endpoint,
        calls: list[_BatchCall],
    ) -> tuple[list[Any | RPCError] | None, Exception | None]:
        """
        Attempt the batch once on `ep`.

        Returns: (values, exc)
          - values is a list aligned to `calls` (RPCError in-position) on success
          - exc is non-None on retryable failure or freshness rejection
          - raises non-retryable exceptions

        Semantics:
          - TransportError (and other retryable exceptions) are returned as exc and mark_failure.
          - Internal tip probe RPCError is treated as an attempt failure (rotate).
          - Per-call RPCError results are returned in-position (not raised).
          - If any per-call freshness rejects, the entire batch is rejected and retried.
        """
        if not calls:
            return [], None

        # If any call requests freshness, we need a tip probe attempt.
        wants_freshness = any(c.freshness is not None for c in calls)

        required_tip = self._best_tip_snapshot()

        try:
            if self.desired_pool_size == 0:
                # Primary-only mode: no tip probing, thus freshness is effectively ignored.
                resp = ep.request_batch(
                    *[(c.method, c.params, c.formatter) for c in calls]  # type: ignore[arg-type]
                )
                self._mark_success(ep)
                # resp already has RPCError in-position
                return resp, None

            # Pool mode: prepend tip probe.
            resp_all = ep.request_batch(
                ("eth_blockNumber", (), to_int),
                *[(c.method, c.params, c.formatter) for c in calls],  # type: ignore[arg-type]
            )

            if not resp_all:
                # Should not happen (we always include tip probe + at least one call),
                # but treat as a retryable-ish failure to be safe.
                raise TransportError("Empty batch response")

            tip_item = resp_all[0]
            user_out = resp_all[1:]

            # Tip probe must succeed for freshness/tip tracking.
            if isinstance(tip_item, RPCError):
                # Treat as an attempt failure (rotate endpoints). Mark as failure for cooldown.
                self._mark_failure(ep, tip_item)
                return None, tip_item

            tip = int(tip_item)
            self._update_tip_and_maybe_demote(ep, tip)
            self._mark_success(ep)

            # Freshness enforcement: if any freshness function rejects, reject whole batch.
            if wants_freshness:
                for i, call in enumerate(calls):
                    if call.freshness is None:
                        continue
                    item = user_out[i]
                    if isinstance(item, RPCError):
                        continue
                    if not call.freshness(item, required_tip, tip):
                        return None, _FreshnessUnmet("Freshness unmet")

            return user_out, None

        except Exception as exc:
            if self.is_retryable_exc(exc):
                self._mark_failure(ep, exc)
                return None, exc
            raise

    # ----------------------------
    # request routing (single)
    # ----------------------------

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        route: str = "pool",
        formatter: Formatter | None = None,
        freshness: Callable[[Any, int, int], bool] | None = None,
    ) -> Any:
        """
        Perform a JSON-RPC request routed either to the merged pool or to the primary.

        - route="pool": RR+retry across (internal_pool + pool_manager best URLs)
        - route="primary": primary-only (raises if primary unset)

        Freshness enforcement (optional):
          freshness(response, required_tip, returned_tip) -> bool

        required_tip is a snapshot of provider best_tip at start of an attempt (concurrency-safe).
        """
        if route not in ("pool", "primary"):
            raise ValueError("route must be 'pool' or 'primary'")

        policy = self.retry_policy_pool

        if route == "primary":
            ep = self._get_primary()

            # In desired_pool_size == 0 mode, freshness is effectively ignored (no returned tip).
            deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS if freshness else 0.0
            last_exc: Exception | None = None

            while True:
                value, exc = self._attempt(ep, method, params, formatter, freshness)
                if exc is None:
                    return value

                if isinstance(exc, _FreshnessUnmet) and time.time() < deadline:
                    time.sleep(policy.backoff_seconds)
                    continue

                last_exc = exc
                raise AllEndpointsFailed(last_exc) from last_exc

        # route == "pool"
        candidates = self._pool_candidates()
        eps = self._eligible_endpoints(candidates)

        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)

        rr_ep = eps[start]
        max_exc_attempts = min(max(1, policy.max_attempts), len(eps))

        # No freshness: bounded attempts across endpoints.
        if freshness is None:
            last_exc: Exception | None = None
            exc_attempts = 0

            for i in range(len(eps)):
                if exc_attempts >= max_exc_attempts:
                    break
                ep = eps[(start + i) % len(eps)]
                value, exc = self._attempt(ep, method, params, formatter, None)
                if exc is None:
                    return value

                last_exc = exc
                exc_attempts += 1
                if i < len(eps) - 1:
                    time.sleep(policy.backoff_seconds)

            raise AllEndpointsFailed(last_exc)

        # Freshness requested: RR once, then prefer highest last_tip, repeat with backoff until cap.
        deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS
        last_exc: Exception | None = None

        while True:
            # Pass order: RR endpoint first, then the rest by last_tip desc.
            sorted_by_last_tip = sorted(list(eps), key=self._last_tip, reverse=True)
            ordered = [rr_ep] + [ep for ep in sorted_by_last_tip if ep is not rr_ep]

            exc_attempts = 0

            for ep in ordered:
                value, exc = self._attempt(ep, method, params, formatter, freshness)
                if exc is None:
                    return value

                last_exc = exc
                if isinstance(exc, _FreshnessUnmet):
                    continue

                # Transport-ish retryable exception: count against budget for this pass.
                exc_attempts += 1
                if exc_attempts >= max_exc_attempts:
                    break

            if time.time() >= deadline:
                raise AllEndpointsFailed(last_exc) from last_exc

            time.sleep(policy.backoff_seconds)

            # Recompute eligibility each pass.
            eps = self._eligible_endpoints(candidates)
            if not eps:
                raise NoEndpoints("No endpoints available")
            rr_ep = eps[start % len(eps)]

    # ----------------------------
    # request routing (batch)
    # ----------------------------

    def request_batch(
        self,
        calls: list[
            tuple[str, Sequence[Any]]
            | tuple[str, Sequence[Any], Formatter | None]
            | tuple[str, Sequence[Any], Formatter | None, Callable[[Any, int, int], bool] | None]
        ],
        *,
        route: str = "pool",
    ) -> list[Any | RPCError]:
        """
        Perform a JSON-RPC batch request routed either to the merged pool or to the primary.

        Each call tuple is:
          (method, params, formatter=None, freshness=None)

        Return value:
          List aligned to the call order, each element is either:
            - the (optionally formatted) result value, or
            - an RPCError instance if that specific call returned a JSON-RPC error object.

        Raises:
          - TransportError (and other retryable/non-retryable exceptions) via routing logic:
              * primary route wraps failure in AllEndpointsFailed
              * pool route may raise AllEndpointsFailed / NoEndpoints
          - RPCMalformedResponse bubbles from Endpoint.request_batch (batch-level malformed shapes)
        """
        if route not in ("pool", "primary"):
            raise ValueError("route must be 'pool' or 'primary'")

        if not calls:
            return []

        parsed: list[_BatchCall] = []
        for call in calls:
            if not isinstance(call, tuple):
                raise TypeError(f"Batch call must be a tuple, got: {type(call).__name__}")

            if len(call) == 2:
                method, params = call  # type: ignore[misc]
                fmt = None
                fresh = None
            elif len(call) == 3:
                method, params, fmt = call  # type: ignore[misc]
                fresh = None
            elif len(call) == 4:
                method, params, fmt, fresh = call  # type: ignore[misc]
            else:
                raise TypeError(
                    "Batch call must be (method, params), (method, params, formatter), "
                    "or (method, params, formatter, freshness)"
                )

            # Normalize params to list/tuple (Endpoint will list() it anyway)
            if not isinstance(params, (list, tuple)):
                raise TypeError("params must be a list or tuple")

            parsed.append(
                _BatchCall(
                    method=method,
                    params=params,
                    formatter=fmt,
                    freshness=fresh,
                )
            )

        policy = self.retry_policy_pool
        wants_freshness = any(c.freshness is not None for c in parsed)

        if route == "primary":
            ep = self._get_primary()

            # In desired_pool_size == 0 mode, freshness is effectively ignored (no returned tip).
            deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS if wants_freshness else 0.0
            last_exc: Exception | None = None

            while True:
                values, exc = self._attempt_batch(ep, parsed)
                if exc is None:
                    # values is non-None here
                    return list(values or [])

                if isinstance(exc, _FreshnessUnmet) and time.time() < deadline:
                    time.sleep(policy.backoff_seconds)
                    continue

                last_exc = exc
                raise AllEndpointsFailed(last_exc) from last_exc

        # route == "pool"
        candidates = self._pool_candidates()
        eps = self._eligible_endpoints(candidates)

        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)

        rr_ep = eps[start]
        max_exc_attempts = min(max(1, policy.max_attempts), len(eps))

        # No freshness: bounded attempts across endpoints.
        if not wants_freshness:
            last_exc: Exception | None = None
            exc_attempts = 0

            for i in range(len(eps)):
                if exc_attempts >= max_exc_attempts:
                    break
                ep = eps[(start + i) % len(eps)]
                values, exc = self._attempt_batch(ep, parsed)
                if exc is None:
                    return list(values or [])

                last_exc = exc
                exc_attempts += 1
                if i < len(eps) - 1:
                    time.sleep(policy.backoff_seconds)

            raise AllEndpointsFailed(last_exc)

        # Freshness requested: RR once, then prefer highest last_tip, repeat with backoff until cap.
        deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS
        last_exc: Exception | None = None

        while True:
            sorted_by_last_tip = sorted(list(eps), key=self._last_tip, reverse=True)
            ordered = [rr_ep] + [ep for ep in sorted_by_last_tip if ep is not rr_ep]

            exc_attempts = 0

            for ep in ordered:
                values, exc = self._attempt_batch(ep, parsed)
                if exc is None:
                    return list(values or [])

                last_exc = exc
                if isinstance(exc, _FreshnessUnmet):
                    continue

                # Count non-freshness failures against this pass' budget
                exc_attempts += 1
                if exc_attempts >= max_exc_attempts:
                    break

            if time.time() >= deadline:
                raise AllEndpointsFailed(last_exc) from last_exc

            time.sleep(policy.backoff_seconds)

            eps = self._eligible_endpoints(candidates)
            if not eps:
                raise NoEndpoints("No endpoints available")
            rr_ep = eps[start % len(eps)]
