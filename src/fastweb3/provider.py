# src/fastweb3/provider.py
from __future__ import annotations

import queue
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
from .middleware import MiddlewareContext, ProviderMiddleware, _apply_default_middlewares
from .rpc_pool import PoolManager
from .utils import normalize_target


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

    # Cooldown due to being the "loser" in a hedged race
    slow_cooldown_until: float = 0.0

    # Last tip observed from this endpoint target (LB backends may vary)
    last_tip: int | None = None


class _FreshnessUnmet(TransportError):
    """Internal sentinel for 'response received, but endpoint tip is too stale for this call'."""


@dataclass(frozen=True)
class _AttemptOutcome:
    value: Any | None
    exc: Exception | None
    returned_tip: int | None


@dataclass(frozen=True)
class _BatchAttemptOutcome:
    values: list[Any | RPCError] | None
    exc: Exception | None
    returned_tip: int | None


@dataclass(frozen=True)
class _RPCErrorRetryDecision:
    """Internal instruction for how to treat a single RPCError result.

    Plumbing-only for now. The default decision is a no-op that keeps current
    behavior: accept RPCError results immediately.

    In the future, heuristics can request:
      - retrying this call on a different endpoint
      - demoting the endpoint that produced the RPCError before retrying
    """

    retry: bool = False
    demote_current_endpoint: bool = False


class Provider:
    """
    Routes requests across:
      - a primary endpoint (explicit, stable identity)
      - an internal pool (per-instance, user-supplied, permanent)
      - a shared PoolManager (per-chain public pool intelligence)

    Pool routing:
      candidates = internal_pool + pool_manager.best_urls(needed)
      where needed fills up to desired_pool_size

    Tip tracking:
      - Every pooled request is executed as a batch: [eth_blockNumber, userCall]
      - Track the best (highest) tip observed across any endpoint
      - Endpoints that report a tip lower than best are temporarily demoted (cooldown)
      - Track per-endpoint last_tip (for preferential retries)

    Freshness enforcement (optional):
      - request() accepts `freshness(response, required_tip, returned_tip) -> bool`
      - required_tip is snapshotted at the start of each attempt (concurrency-safe)
      - If freshness rejects, rotate endpoints preferring highest last_tip, backoff briefly, retry

    Hedging (pool route only):
      - If `hedge_after_seconds` is set, fire attempt #1 immediately.
      - If attempt #1 has not completed within `hedge_after_seconds`, fire attempt #2.
      - Return whichever completes first.
      - If attempt #2 wins, demote the loser endpoint.
      - Only the winning attempt mutates Provider state.
    """

    _FRESHNESS_WAIT_CAP_SECONDS: float = 5.0
    _HEDGE_MIN_DELAY_SECONDS: float = 0.05  # guardrail: below this hedges basically everything

    def __init__(
        self,
        internal_endpoints: Sequence[str] | None = None,
        *,
        pool_manager: PoolManager | None = None,
        desired_pool_size: int = 6,
        retry_policy_pool: RetryPolicy | None = None,
        is_retryable_exc: Callable[[Exception], bool] = _default_is_retryable_exc,
        hedge_after_seconds: float | None = 0.2,
        hedge_slow_cooldown_seconds: float = 10.0,
    ) -> None:
        self._lock = threading.Lock()
        self._rr = 0

        # Per-provider middleware stack.
        self._middlewares: list[ProviderMiddleware] = []

        internal_endpoints_list = list(internal_endpoints or [])

        if pool_manager is None:
            desired_pool_size = 0
        self.desired_pool_size = max(
            len(internal_endpoints_list),
            max(0, int(desired_pool_size)),
        )

        self.pool_manager = pool_manager
        self.retry_policy_pool = retry_policy_pool or RetryPolicy(max_attempts=3)
        self.is_retryable_exc = is_retryable_exc

        if hedge_after_seconds is not None:
            hedge_after_seconds = float(hedge_after_seconds)
            if hedge_after_seconds < self._HEDGE_MIN_DELAY_SECONDS:
                raise ValueError(
                    f"hedge_after_seconds must be >= {self._HEDGE_MIN_DELAY_SECONDS} or None"
                )

        self.hedge_after_seconds = hedge_after_seconds
        self.hedge_slow_cooldown_seconds = float(hedge_slow_cooldown_seconds)

        # Internal pool endpoint targets (normalized, insertion-ordered) + membership set.
        self._internal_targets: list[str] = []
        self._internal_seen: set[str] = set()

        # Endpoint cache for any target we might touch (internal or manager).
        self._eps_by_target: dict[str, Endpoint] = {}
        self._state: dict[Endpoint, _EndpointState] = {}

        # Primary is a specific Endpoint instance (stable identity), or None.
        self._primary: Endpoint | None = None

        # Best (highest) chain tip we've observed across any endpoint.
        self._best_tip: int | None = None

        for t in internal_endpoints_list:
            self.add_endpoint(t, priority=False)

        # Global default middlewares are copied onto this Provider instance once at init.
        _apply_default_middlewares(self)

    # ----------------------------
    # RPCError retry plumbing (no-op for now)
    # ----------------------------
    def _decide_rpc_error_retry(
        self,
        *,
        ep: Endpoint,
        call: _BatchCall,
        err: RPCError,
        returned_tip: int | None,
        route: str,
    ) -> _RPCErrorRetryDecision:
        """Return a retry decision for a single RPCError response.

        Default is a no-op: accept the RPCError immediately.

        Notes for future heuristics:
          - `route` indicates whether this came from the pool route or primary route.
          - `returned_tip` is the observed block height for the attempt (if any). For
            partial retries within a batch, callers should pin retried calls to this
            same block height to avoid mixed-block results.
        """
        _ = (ep, call, err, returned_tip, route)
        return _RPCErrorRetryDecision()

    def _pin_calls_to_tip(self, calls: list[_BatchCall], tip: int) -> list[_BatchCall]:
        """Return calls rewritten to target a specific block number.

        When partially retrying a batch, we must ensure the retried subset targets the
        same block as the original successful batch attempt; otherwise a moving tip
        can yield internally inconsistent results across the batch.

        This function rewrites only methods that accept a block tag/number. It will:
          - Replace a block tag of "latest" with `hex(tip)`.
          - Add a missing block parameter (where supported) as `hex(tip)`.

        It intentionally does NOT rewrite calls that already target a specific block
        number/hash, or special tags like "earliest" / "pending".
        """
        tip_hex = hex(tip)

        def _pin_block_param(param: Any) -> Any:
            # Only pin simple string tags. We intentionally do NOT support EIP-1898
            # block objects here because they are rare in practice and callers using
            # them are already explicitly pinning to a specific block context.
            if isinstance(param, str):
                return tip_hex if param == "latest" else param
            return param

        pinned: list[_BatchCall] = []

        for call in calls:
            method = call.method
            params = list(call.params) if isinstance(call.params, tuple) else list(call.params)

            # Methods where the block param is the 2nd argument (index 1)
            if method in {
                "eth_call",
                "eth_getBalance",
                "eth_getCode",
                "eth_getTransactionCount",
            }:
                if len(params) < 2:
                    params.append(tip_hex)
                else:
                    params[1] = _pin_block_param(params[1])

            # Methods where the block param is the 3rd argument (index 2)
            elif method in {"eth_getStorageAt"}:
                if len(params) >= 3:
                    params[2] = _pin_block_param(params[2])

            elif method in {"eth_getProof"}:
                if len(params) >= 3:
                    params[2] = _pin_block_param(params[2])

            # Methods where the block param is the 1st argument (index 0)
            elif method in {
                "eth_getBlockByNumber",
                "eth_getBlockTransactionCountByNumber",
                "eth_getUncleCountByBlockNumber",
            }:
                if len(params) >= 1:
                    params[0] = _pin_block_param(params[0])

            # eth_feeHistory: newestBlock is index 1
            elif method in {"eth_feeHistory"}:
                if len(params) >= 2:
                    params[1] = _pin_block_param(params[1])

            # Everything else: no change
            pinned.append(
                _BatchCall(
                    method=call.method,
                    params=params,
                    formatter=call.formatter,
                    freshness=call.freshness,
                )
            )

        return pinned

    def _maybe_retry_rpc_errors_in_batch(
        self,
        *,
        ep: Endpoint,
        calls: list[_BatchCall],
        results: list[Any | RPCError],
        returned_tip: int | None,
        route: str,
    ) -> list[Any | RPCError]:
        """Apply per-item RPCError retry heuristics to a batch result set.

        This evaluates the retry decision *per individual RPCError item* in the batch.
        If any items are selected for retry, it will:
          - optionally demote the endpoint that produced the RPCError(s)
          - rerun only the selected calls on a different eligible endpoint
          - merge the retried results back into the original `results` list

        If all retry attempts fail (transport-wise), the original `results` are returned.
        """
        retry_indices: list[int] = []
        demote = False

        # Evaluate decision per-item (only for actual RPCError items)
        for i, item in enumerate(results[: len(calls)]):
            if not isinstance(item, RPCError):
                continue

            decision = self._decide_rpc_error_retry(
                ep=ep,
                call=calls[i],
                err=item,
                returned_tip=returned_tip,
                route=route,
            )
            if decision.retry:
                retry_indices.append(i)
                demote = demote or decision.demote_current_endpoint

        # Default behavior: accept everything as-is.
        if not retry_indices:
            return results

        # Optionally demote the producing endpoint before retrying elsewhere.
        if demote:
            # Treat this as an endpoint failure for cooldown purposes.
            self._mark_failure(ep, TransportError("RPCError selected for retry"))

        # Build retry-only batch, pinned to the same tip if available.
        retry_calls = [calls[i] for i in retry_indices]
        if returned_tip is not None:
            retry_calls = self._pin_calls_to_tip(retry_calls, returned_tip)

        # Pick alternative endpoints (exclude the producing endpoint).
        try:
            candidates = self._pool_candidates()
        except Exception:
            # If we can't even enumerate candidates, fall back to original results.
            return results

        eligible = [e for e in self._eligible_endpoints(candidates) if e is not ep]
        if not eligible:
            return results

        # Prefer endpoints with higher recently-observed tips.
        ordered = sorted(eligible, key=self._last_tip, reverse=True)

        # Attempt the retry-only batch on alternative endpoints until one succeeds.
        for retry_ep in ordered:
            values2, exc2, _ = self._attempt_batch(retry_ep, retry_calls)
            if exc2 is not None:
                continue

            retried = list(values2 or [])
            # Merge back in positional order corresponding to retry_indices
            for j, idx in enumerate(retry_indices):
                if j >= len(retried):
                    break
                results[idx] = retried[j]
            return results

        return results

    # ----------------------------
    # middleware
    # ----------------------------

    def add_middleware(self, mw: ProviderMiddleware, *, prepend: bool = False) -> None:
        with self._lock:
            if prepend:
                self._middlewares.insert(0, mw)
            else:
                self._middlewares.append(mw)

    def _run_middlewares_before(
        self, ctx: MiddlewareContext, calls: list[_BatchCall]
    ) -> list[_BatchCall]:
        with self._lock:
            mws = list(self._middlewares)
        for mw in mws:
            fn = getattr(mw, "before_request", None)
            if fn is not None:
                calls = fn(ctx, calls)
        return calls

    def _run_middlewares_after(
        self,
        ctx: MiddlewareContext,
        calls: list[_BatchCall],
        results: list[Any | RPCError],
    ) -> list[Any | RPCError]:
        with self._lock:
            mws = list(self._middlewares)
        for mw in reversed(mws):
            fn = getattr(mw, "after_request", None)
            if fn is not None:
                results = fn(ctx, calls, results)
        return results

    def _run_middlewares_on_exception(
        self,
        ctx: MiddlewareContext,
        calls: list[_BatchCall],
        exc: Exception,
    ) -> list[Any | RPCError] | Exception:
        with self._lock:
            mws = list(self._middlewares)
        out: list[Any | RPCError] | Exception = exc
        for mw in reversed(mws):
            fn = getattr(mw, "on_exception", None)
            if fn is None:
                continue

            # Only give the hook a chance if we're still in "exception mode".
            if not isinstance(out, Exception):
                break

            try:
                out = fn(ctx, calls, out)
            except Exception as e:
                out = e
        return out

    # ----------------------------
    # endpoint cache helpers
    # ----------------------------

    def _get_or_create_endpoint(self, target: str) -> Endpoint:
        nt = normalize_target(target)
        with self._lock:
            ep = self._eps_by_target.get(nt)
            if ep is not None:
                return ep
            ep = Endpoint(nt)
            self._eps_by_target[nt] = ep
            self._state[ep] = _EndpointState()
            return ep

    # ----------------------------
    # primary management
    # ----------------------------

    def set_primary(self, target: str) -> None:
        """
        Set the primary endpoint to `target`.

        Note: this does NOT add the target to the internal pool.
        """
        ep = self._get_or_create_endpoint(target)
        with self._lock:
            self._primary = ep

    def clear_primary(self) -> None:
        with self._lock:
            self._primary = None

    def primary_endpoint(self) -> str | None:
        with self._lock:
            return self._primary.target if self._primary is not None else None

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

    def add_endpoint(self, target: str, *, priority: bool = False) -> None:
        """
        Add an endpoint target to the internal pool (per-instance, permanent).
        Deduped by normalized target.
        """
        nt = normalize_target(target)
        with self._lock:
            if nt in self._internal_seen:
                return
            self._internal_seen.add(nt)
            if priority:
                self._internal_targets.insert(0, nt)
            else:
                self._internal_targets.append(nt)

        self._get_or_create_endpoint(nt)

    def remove_endpoint(self, target: str) -> None:
        """
        Remove an endpoint target from the internal pool. No-op if missing.

        Does not delete the Endpoint from cache (it may be referenced by primary or manager).
        If the removed target is the primary, primary is cleared.
        """
        nt = normalize_target(target)
        with self._lock:
            if nt not in self._internal_seen:
                return
            self._internal_seen.remove(nt)
            self._internal_targets = [t for t in self._internal_targets if t != nt]

            if self._primary is not None and self._primary.target == nt:
                self._primary = None

    def internal_endpoints(self) -> list[str]:
        with self._lock:
            return list(self._internal_targets)

    # ----------------------------
    # lifecycle
    # ----------------------------

    def close(self) -> None:
        with self._lock:
            eps = list(self._eps_by_target.values())
            self._eps_by_target.clear()
            self._state.clear()
            self._internal_targets.clear()
            self._internal_seen.clear()
            self._primary = None
            self._best_tip = None

        for ep in eps:
            try:
                ep.close()
            except Exception:
                pass

    # ----------------------------
    # state helpers
    # ----------------------------

    def _best_tip_snapshot(self) -> int:
        with self._lock:
            return int(self._best_tip or 0)

    def _last_tip(self, ep: Endpoint) -> int:
        with self._lock:
            st = self._state.get(ep)
            return int(st.last_tip or 0) if st is not None else 0

    def _is_cooldown_active(self, ep: Endpoint, now: float) -> bool:
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                return False
            return (
                st.error_cooldown_until > now
                or st.tip_cooldown_until > now
                or st.slow_cooldown_until > now
            )

    def _mark_success(self, ep: Endpoint) -> None:
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                return
            st.failures = 0
            st.error_cooldown_until = 0.0

    def _mark_failure(self, ep: Endpoint, exc: Exception) -> None:
        now = time.time()
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                return
            st.failures += 1
            # basic exponential-ish backoff: 0.1, 0.2, 0.4, ... capped
            delay = min(10.0, 0.1 * (2 ** min(6, st.failures - 1)))
            st.error_cooldown_until = max(st.error_cooldown_until, now + delay)

    def _mark_slow(self, ep: Endpoint) -> None:
        now = time.time()
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                return
            st.slow_cooldown_until = max(
                st.slow_cooldown_until, now + self.hedge_slow_cooldown_seconds
            )

    def _update_tip_and_maybe_demote(self, ep: Endpoint, tip: int) -> None:
        now = time.time()
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                return

            st.last_tip = int(tip)

            if self._best_tip is None or tip > self._best_tip:
                self._best_tip = int(tip)
                return

            best = int(self._best_tip)
            if tip < best:
                # stale -> temporary demotion
                st.tip_cooldown_until = max(st.tip_cooldown_until, now + 2.0)

    # ----------------------------
    # endpoint selection
    # ----------------------------

    def _eligible_endpoints(self, eps: list[Endpoint]) -> list[Endpoint]:
        now = time.time()
        out: list[Endpoint] = []
        for ep in eps:
            if not self._is_cooldown_active(ep, now):
                out.append(ep)
        return out

    def _pool_candidates(self) -> list[Endpoint]:
        with self._lock:
            internal = list(self._internal_targets)
            primary = self._primary

        needed = max(0, self.desired_pool_size - len(internal))
        manager_targets: list[str] = []
        if needed > 0 and self.pool_manager is not None:
            await_first = not (internal or primary)
            manager_targets = self.pool_manager.best_urls(needed, await_first)

        seen: set[str] = set()
        merged: list[str] = []

        for t in internal + manager_targets:
            nt = normalize_target(t)
            if nt in seen:
                continue
            seen.add(nt)
            merged.append(nt)

        if not merged:
            if primary is not None:
                return [primary]
            raise NoEndpoints("No endpoints available")

        return [self._get_or_create_endpoint(t) for t in merged]

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
        outcome = self._attempt_raw(ep, method, params, formatter, freshness)
        return self._apply_outcome_single(ep, outcome)

    # ----------------------------
    # attempt (batch)
    # ----------------------------

    def _attempt_batch(
        self,
        ep: Endpoint,
        calls: list[_BatchCall],
    ) -> tuple[list[Any | RPCError] | None, Exception | None, int | None]:
        """Attempt a batch request and preserve the observed tip (if any)."""
        outcome = self._attempt_batch_raw(ep, calls)
        values, exc = self._apply_outcome_batch(ep, outcome)
        return values, exc, outcome.returned_tip

    def _attempt_raw(
        self,
        ep: Endpoint,
        method: str,
        params: list[Any] | tuple[Any, ...],
        formatter: Formatter | None,
        freshness: Callable[[Any, int, int], bool] | None,
    ) -> _AttemptOutcome:
        required_tip = self._best_tip_snapshot()

        try:
            if self.desired_pool_size == 0:
                result = ep.request(method, params, formatter)
                return _AttemptOutcome(result, None, None)

            tip, result = ep.request_batch(
                ("eth_blockNumber", (), to_int),
                (method, params, formatter),
            )
            for item in (result, tip):
                if isinstance(item, RPCError):
                    raise item

            if freshness is None:
                return _AttemptOutcome(result, None, tip)

            if freshness(result, required_tip, tip):
                return _AttemptOutcome(result, None, tip)

            return _AttemptOutcome(None, _FreshnessUnmet("Freshness unmet"), tip)

        except RPCError:
            raise

        except Exception as exc:
            if self.is_retryable_exc(exc):
                return _AttemptOutcome(None, exc, None)
            raise

    def _attempt_batch_raw(self, ep: Endpoint, calls: list[_BatchCall]) -> _BatchAttemptOutcome:
        if not calls:
            return _BatchAttemptOutcome([], None, None)

        wants_freshness = any(c.freshness is not None for c in calls)
        required_tip = self._best_tip_snapshot()

        try:
            if self.desired_pool_size == 0:
                resp = ep.request_batch(*[(c.method, c.params, c.formatter) for c in calls])  # type: ignore[arg-type]
                return _BatchAttemptOutcome(list(resp), None, None)

            resp_all = ep.request_batch(
                ("eth_blockNumber", (), to_int),
                *[(c.method, c.params, c.formatter) for c in calls],  # type: ignore[arg-type]
            )

            if not resp_all:
                raise TransportError("Empty batch response")

            tip_item = resp_all[0]
            user_out = list(resp_all[1:])

            if isinstance(tip_item, RPCError):
                return _BatchAttemptOutcome(None, tip_item, None)

            tip = tip_item

            if wants_freshness:
                for i, call in enumerate(calls):
                    if call.freshness is None:
                        continue
                    item = user_out[i]
                    if isinstance(item, RPCError):
                        continue
                    if not call.freshness(item, required_tip, tip):
                        return _BatchAttemptOutcome(None, _FreshnessUnmet("Freshness unmet"), tip)

            return _BatchAttemptOutcome(user_out, None, tip)

        except Exception as exc:
            if self.is_retryable_exc(exc):
                return _BatchAttemptOutcome(None, exc, None)
            raise

    # ----------------------------
    # apply-outcome helpers (single winner mutates Provider state)
    # ----------------------------

    def _apply_outcome_single(
        self, ep: Endpoint, outcome: _AttemptOutcome
    ) -> tuple[Any | None, Exception | None]:
        if outcome.exc is None:
            if outcome.returned_tip is not None:
                self._update_tip_and_maybe_demote(ep, outcome.returned_tip)
            self._mark_success(ep)
            return outcome.value, None

        if isinstance(outcome.exc, _FreshnessUnmet):
            if outcome.returned_tip is not None:
                self._update_tip_and_maybe_demote(ep, outcome.returned_tip)
            self._mark_success(ep)
            return None, outcome.exc

        if isinstance(outcome.exc, RPCError):
            raise outcome.exc

        if self.is_retryable_exc(outcome.exc):
            self._mark_failure(ep, outcome.exc)
            return None, outcome.exc

        raise outcome.exc

    def _apply_outcome_batch(
        self, ep: Endpoint, outcome: _BatchAttemptOutcome
    ) -> tuple[list[Any | RPCError] | None, Exception | None]:
        if outcome.exc is None:
            if outcome.returned_tip is not None:
                self._update_tip_and_maybe_demote(ep, outcome.returned_tip)
            self._mark_success(ep)
            return list(outcome.values or []), None

        if isinstance(outcome.exc, _FreshnessUnmet):
            if outcome.returned_tip is not None:
                self._update_tip_and_maybe_demote(ep, outcome.returned_tip)
            self._mark_success(ep)
            return None, outcome.exc

        # In batch mode, an attempt-level RPCError (e.g. tip probe) is treated as a failure + retry.
        if isinstance(outcome.exc, RPCError):
            self._mark_failure(ep, outcome.exc)
            return None, outcome.exc

        if self.is_retryable_exc(outcome.exc):
            self._mark_failure(ep, outcome.exc)
            return None, outcome.exc

        raise outcome.exc

    # ----------------------------
    # hedging helpers
    # ----------------------------

    def _race_single(
        self,
        ep1: Endpoint,
        ep2: Endpoint,
        call: _BatchCall,
    ) -> tuple[_AttemptOutcome, Endpoint, Endpoint | None, bool]:
        q: queue.Queue[tuple[Endpoint, _AttemptOutcome]] = queue.Queue()

        def run(ep: Endpoint) -> None:
            try:
                outcome = self._attempt_raw(
                    ep,
                    call.method,
                    call.params,
                    call.formatter,
                    call.freshness,
                )
            except Exception as exc:
                outcome = _AttemptOutcome(None, exc, None)
            q.put((ep, outcome))

        t1 = threading.Thread(target=run, args=(ep1,), daemon=True)
        t1.start()

        try:
            winner_ep, winner_outcome = q.get(timeout=float(self.hedge_after_seconds))
            return winner_outcome, winner_ep, None, False
        except queue.Empty:
            pass

        t2 = threading.Thread(target=run, args=(ep2,), daemon=True)
        t2.start()

        winner_ep, winner_outcome = q.get()
        loser_ep = ep2 if winner_ep is ep1 else ep1
        winner_was_second = winner_ep is ep2
        return winner_outcome, winner_ep, loser_ep, winner_was_second

    def _race_batch(
        self,
        ep1: Endpoint,
        ep2: Endpoint,
        calls: list[_BatchCall],
    ) -> tuple[_BatchAttemptOutcome, Endpoint, Endpoint | None, bool]:
        q: queue.Queue[tuple[Endpoint, _BatchAttemptOutcome]] = queue.Queue()

        def run(ep: Endpoint) -> None:
            try:
                outcome = self._attempt_batch_raw(ep, calls)
            except Exception as exc:
                outcome = _BatchAttemptOutcome(None, exc, None)
            q.put((ep, outcome))

        t1 = threading.Thread(target=run, args=(ep1,), daemon=True)
        t1.start()

        try:
            winner_ep, winner_outcome = q.get(timeout=float(self.hedge_after_seconds))
            return winner_outcome, winner_ep, None, False
        except queue.Empty:
            pass

        t2 = threading.Thread(target=run, args=(ep2,), daemon=True)
        t2.start()

        winner_ep, winner_outcome = q.get()
        loser_ep = ep2 if winner_ep is ep1 else ep1
        winner_was_second = winner_ep is ep2
        return winner_outcome, winner_ep, loser_ep, winner_was_second

    # ----------------------------
    # core execution (single)
    # ----------------------------

    def _execute_single(self, call: _BatchCall, *, route: str) -> Any:
        policy = self.retry_policy_pool
        wants_freshness = call.freshness is not None

        if route == "primary":
            ep = self._get_primary()
            deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS if wants_freshness else 0.0
            last_exc: Exception | None = None

            while True:
                value, exc = self._attempt(
                    ep, call.method, call.params, call.formatter, call.freshness
                )
                if exc is None:
                    return value

                if isinstance(exc, _FreshnessUnmet) and time.time() < deadline:
                    time.sleep(policy.backoff_seconds)
                    continue

                last_exc = exc
                raise AllEndpointsFailed(last_exc) from last_exc

        candidates = self._pool_candidates()
        eps = self._eligible_endpoints(candidates)

        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)

        rr_ep = eps[start]
        max_exc_attempts = min(max(1, policy.max_attempts), len(eps))

        if not wants_freshness:
            last_exc: Exception | None = None
            exc_attempts = 0
            skip_n = 0

            if (
                self.hedge_after_seconds is not None
                and len(eps) >= 2
                and exc_attempts < max_exc_attempts
            ):
                outcome, winner_ep, loser_ep, winner_was_second = self._race_single(
                    eps[start], eps[(start + 1) % len(eps)], call
                )

                skip_n = 2 if loser_ep is not None else 1

                value, exc = self._apply_outcome_single(winner_ep, outcome)
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    return value

                last_exc = exc
                exc_attempts += 1
                if len(eps) > skip_n:
                    time.sleep(policy.backoff_seconds)

            for i in range(len(eps)):
                if exc_attempts >= max_exc_attempts:
                    break
                if i < skip_n:
                    continue

                ep = eps[(start + i) % len(eps)]
                value, exc = self._attempt(
                    ep, call.method, call.params, call.formatter, call.freshness
                )
                if exc is None:
                    return value

                last_exc = exc
                exc_attempts += 1
                if i < len(eps) - 1:
                    time.sleep(policy.backoff_seconds)

            raise AllEndpointsFailed(last_exc)

        deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS
        last_exc: Exception | None = None

        while True:
            sorted_by_last_tip = sorted(list(eps), key=self._last_tip, reverse=True)
            ordered = [rr_ep] + [ep for ep in sorted_by_last_tip if ep is not rr_ep]

            exc_attempts = 0
            skip_n = 0
            hedged_eps: tuple[Endpoint, Endpoint] | None = None

            if self.hedge_after_seconds is not None and len(ordered) >= 2:
                ep1, ep2 = ordered[0], ordered[1]
                hedged_eps = (ep1, ep2)

                outcome, winner_ep, loser_ep, winner_was_second = self._race_single(ep1, ep2, call)

                skip_n = 2 if loser_ep is not None else 1

                value, exc = self._apply_outcome_single(winner_ep, outcome)
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    return value

                last_exc = exc
                if isinstance(exc, _FreshnessUnmet):
                    pass
                else:
                    exc_attempts += 1

            for idx, ep in enumerate(ordered):
                if hedged_eps is not None and idx < skip_n and ep in hedged_eps:
                    continue

                value, exc = self._attempt(
                    ep,
                    call.method,
                    call.params,
                    call.formatter,
                    call.freshness,
                )
                if exc is None:
                    return value

                last_exc = exc
                if isinstance(exc, _FreshnessUnmet):
                    continue

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

    # ----------------------------
    # core execution (batch)
    # ----------------------------

    def _execute_batch(self, parsed: list[_BatchCall], *, route: str) -> list[Any | RPCError]:
        policy = self.retry_policy_pool
        wants_freshness = any(c.freshness is not None for c in parsed)

        if route == "primary":
            ep = self._get_primary()
            deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS if wants_freshness else 0.0
            last_exc: Exception | None = None

            while True:
                values, exc, returned_tip = self._attempt_batch(ep, parsed)
                if exc is None:
                    results = list(values or [])
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=ep,
                        calls=parsed,
                        results=results,
                        returned_tip=returned_tip,
                        route=route,
                    )

                if isinstance(exc, _FreshnessUnmet) and time.time() < deadline:
                    time.sleep(policy.backoff_seconds)
                    continue

                last_exc = exc
                raise AllEndpointsFailed(last_exc) from last_exc

        candidates = self._pool_candidates()
        eps = self._eligible_endpoints(candidates)

        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)

        rr_ep = eps[start]
        max_exc_attempts = min(max(1, policy.max_attempts), len(eps))

        if not wants_freshness:
            last_exc: Exception | None = None
            exc_attempts = 0
            skip_n = 0

            if (
                self.hedge_after_seconds is not None
                and len(eps) >= 2
                and exc_attempts < max_exc_attempts
            ):
                outcome, winner_ep, loser_ep, winner_was_second = self._race_batch(
                    eps[start], eps[(start + 1) % len(eps)], parsed
                )

                skip_n = 2 if loser_ep is not None else 1

                values, exc = self._apply_outcome_batch(winner_ep, outcome)
                returned_tip = outcome.returned_tip
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    results = list(values or [])
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=winner_ep,
                        calls=parsed,
                        results=results,
                        returned_tip=returned_tip,
                        route=route,
                    )

                last_exc = exc
                exc_attempts += 1
                if len(eps) > skip_n:
                    time.sleep(policy.backoff_seconds)

            for i in range(len(eps)):
                if exc_attempts >= max_exc_attempts:
                    break
                if i < skip_n:
                    continue

                ep = eps[(start + i) % len(eps)]
                values, exc, returned_tip = self._attempt_batch(ep, parsed)
                if exc is None:
                    results = list(values or [])
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=ep,
                        calls=parsed,
                        results=results,
                        returned_tip=returned_tip,
                        route=route,
                    )

                last_exc = exc
                exc_attempts += 1
                if i < len(eps) - 1:
                    time.sleep(policy.backoff_seconds)

            raise AllEndpointsFailed(last_exc)

        deadline = time.time() + self._FRESHNESS_WAIT_CAP_SECONDS
        last_exc: Exception | None = None

        while True:
            sorted_by_last_tip = sorted(list(eps), key=self._last_tip, reverse=True)
            ordered = [rr_ep] + [ep for ep in sorted_by_last_tip if ep is not rr_ep]

            exc_attempts = 0
            skip_n = 0
            hedged_eps: tuple[Endpoint, Endpoint] | None = None

            if self.hedge_after_seconds is not None and len(ordered) >= 2:
                ep1, ep2 = ordered[0], ordered[1]
                hedged_eps = (ep1, ep2)

                outcome, winner_ep, loser_ep, winner_was_second = self._race_batch(ep1, ep2, parsed)

                skip_n = 2 if loser_ep is not None else 1

                values, exc = self._apply_outcome_batch(winner_ep, outcome)
                returned_tip = outcome.returned_tip
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    results = list(values or [])
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=winner_ep,
                        calls=parsed,
                        results=results,
                        returned_tip=returned_tip,
                        route=route,
                    )

                last_exc = exc
                if isinstance(exc, _FreshnessUnmet):
                    pass
                else:
                    exc_attempts += 1

            for idx, ep in enumerate(ordered):
                if hedged_eps is not None and idx < skip_n and ep in hedged_eps:
                    continue

                values, exc, returned_tip = self._attempt_batch(ep, parsed)
                if exc is None:
                    results = list(values or [])
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=ep,
                        calls=parsed,
                        results=results,
                        returned_tip=returned_tip,
                        route=route,
                    )

                last_exc = exc
                if isinstance(exc, _FreshnessUnmet):
                    continue

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
        if route not in ("pool", "primary"):
            raise ValueError("route must be 'pool' or 'primary'")

        calls = [_BatchCall(method=method, params=params, formatter=formatter, freshness=freshness)]

        ctx = MiddlewareContext(state={})
        calls = self._run_middlewares_before(ctx, calls)

        # For request(), middleware must keep this a single call.
        if len(calls) != 1:
            raise ValueError("Middleware transformed request() into a batch; use request_batch()")

        try:
            out = self._execute_single(calls[0], route=route)
        except Exception as exc:
            recovered = self._run_middlewares_on_exception(ctx, calls, exc)
            if isinstance(recovered, Exception):
                raise recovered
            if not recovered:
                raise TransportError("Empty batch response")
            item = recovered[0]
            if isinstance(item, RPCError):
                raise item
            out = item

        results = self._run_middlewares_after(ctx, calls, [out])
        if not results:
            raise TransportError("Empty batch response")
        item = results[0]
        if isinstance(item, RPCError):
            raise item
        return item

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

        ctx = MiddlewareContext(state={})
        parsed = self._run_middlewares_before(ctx, parsed)

        try:
            out = self._execute_batch(parsed, route=route)
        except Exception as exc:
            recovered = self._run_middlewares_on_exception(ctx, parsed, exc)
            if isinstance(recovered, Exception):
                raise recovered
            out = recovered

        out = self._run_middlewares_after(ctx, parsed, list(out))
        return list(out)
