"""Request execution logic for `fastweb3.provider.Provider`.

This mixin contains the core logic for single and batch execution, including:

* endpoint retries and cooldowns
* optional hedged requests
* optional "freshness" enforcement using block tip pinning
* optional RPC error retry for safe read methods
"""

from __future__ import annotations

import time
from typing import Any, Sequence

from ..errors import AllEndpointsFailed, NoEndpoints, RPCError, TransportError
from ..middleware import MiddlewareContext
from .types import _BatchCall, _FreshnessUnmet, _RPCErrorObservation


class ExecutionMixin:
    """Mixin that implements Provider.request and Provider.request_batch."""

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
        eps = self._eligible_endpoints(candidates, {call.method})
        if not eps:
            raise NoEndpoints("No endpoints available")
        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)
        rr_ep = eps[start]
        max_exc_attempts = min(max(1, policy.max_attempts), len(eps))
        rpc_error_history: list[_RPCErrorObservation] = []

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
                value, exc, rpc_error_history = self._apply_outcome_single_with_rpc_error_retry(
                    winner_ep, call, outcome, rpc_error_history
                )
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
                value, exc, rpc_error_history = self._attempt_with_rpc_error_retry(
                    ep, call, rpc_error_history
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
            hedged_eps: tuple[object, object] | None = None

            if self.hedge_after_seconds is not None and len(ordered) >= 2:
                ep1, ep2 = ordered[0], ordered[1]
                hedged_eps = (ep1, ep2)
                outcome, winner_ep, loser_ep, winner_was_second = self._race_single(ep1, ep2, call)
                skip_n = 2 if loser_ep is not None else 1
                value, exc, rpc_error_history = self._apply_outcome_single_with_rpc_error_retry(
                    winner_ep, call, outcome, rpc_error_history
                )
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    return value
                last_exc = exc
                if not isinstance(exc, _FreshnessUnmet):
                    exc_attempts += 1

            for idx, ep in enumerate(ordered):
                if hedged_eps is not None and idx < skip_n and ep in hedged_eps:
                    continue
                value, exc, rpc_error_history = self._attempt_with_rpc_error_retry(
                    ep, call, rpc_error_history
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
            eps = self._eligible_endpoints(candidates, {call.method})
            if not eps:
                raise NoEndpoints("No endpoints available")
            rr_ep = eps[start % len(eps)]

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
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=ep,
                        calls=parsed,
                        results=list(values or []),
                        returned_tip=returned_tip,
                        route=route,
                    )
                if isinstance(exc, _FreshnessUnmet) and time.time() < deadline:
                    time.sleep(policy.backoff_seconds)
                    continue
                last_exc = exc
                raise AllEndpointsFailed(last_exc) from last_exc

        candidates = self._pool_candidates()
        eps = self._eligible_endpoints(candidates, {c.method for c in parsed})
        if not eps:
            raise NoEndpoints("No endpoints available")
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
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=winner_ep,
                        calls=parsed,
                        results=list(values or []),
                        returned_tip=outcome.returned_tip,
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
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=ep,
                        calls=parsed,
                        results=list(values or []),
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
            hedged_eps: tuple[object, object] | None = None

            if self.hedge_after_seconds is not None and len(ordered) >= 2:
                ep1, ep2 = ordered[0], ordered[1]
                hedged_eps = (ep1, ep2)
                outcome, winner_ep, loser_ep, winner_was_second = self._race_batch(ep1, ep2, parsed)
                skip_n = 2 if loser_ep is not None else 1
                values, exc = self._apply_outcome_batch(winner_ep, outcome)
                if exc is None:
                    if winner_was_second and loser_ep is not None:
                        self._mark_slow(loser_ep)
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=winner_ep,
                        calls=parsed,
                        results=list(values or []),
                        returned_tip=outcome.returned_tip,
                        route=route,
                    )
                last_exc = exc
                if not isinstance(exc, _FreshnessUnmet):
                    exc_attempts += 1

            for idx, ep in enumerate(ordered):
                if hedged_eps is not None and idx < skip_n and ep in hedged_eps:
                    continue
                values, exc, returned_tip = self._attempt_batch(ep, parsed)
                if exc is None:
                    return self._maybe_retry_rpc_errors_in_batch(
                        ep=ep,
                        calls=parsed,
                        results=list(values or []),
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
            eps = self._eligible_endpoints(candidates, {c.method for c in parsed})
            if not eps:
                raise NoEndpoints("No endpoints available")
            rr_ep = eps[start % len(eps)]

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        route: str = "pool",
        formatter=None,
        freshness=None,
    ) -> Any:
        """Execute a single JSON-RPC call.

        Args:
            method: JSON-RPC method name.
            params: JSON-RPC params.
            route: Routing hint: ``"pool"`` uses the pool (internal endpoints
                + pool manager), and ``"primary"`` routes to the primary
                endpoint.
            formatter: Optional formatter applied to the result.
            freshness: Optional freshness predicate
                ``freshness(result, required_tip, returned_tip) -> bool``.

        Returns:
            The result (optionally formatted).

        Raises:
            RPCError: If the call results in a JSON-RPC error response.
            TransportError: For transport-level failures that cannot be
                recovered.
            AllEndpointsFailed: If all eligible endpoints failed.
            NoPrimaryEndpoint: If ``route="primary"`` is used without a
                configured primary.
        """
        if route not in ("pool", "primary"):
            raise ValueError("route must be 'pool' or 'primary'")

        calls = [_BatchCall(method=method, params=params, formatter=formatter, freshness=freshness)]
        ctx = MiddlewareContext(state={})
        calls = self._run_middlewares_before(ctx, calls)
        if len(calls) > 1:
            raise ValueError("Middleware transformed request() into a batch; use request_batch()")

        out: list[Any | RPCError]
        try:
            out = [self._execute_single(calls[0], route=route)] if calls else []
        except Exception as exc:
            recovered = self._run_middlewares_on_exception(ctx, calls, exc)
            if isinstance(recovered, Exception):
                raise recovered
            out = list(recovered)

        results = self._run_middlewares_after(ctx, calls, out)
        if not results:
            raise TransportError("Empty batch response")
        item = results[0]
        if isinstance(item, RPCError):
            raise item
        return item

    def request_batch(
        self,
        calls: list[
            tuple[str, Sequence[Any]]
            | tuple[str, Sequence[Any], object | None]
            | tuple[str, Sequence[Any], object | None, object | None]
        ],
        *,
        route: str = "pool",
    ) -> list[Any | RPCError]:
        """Execute a JSON-RPC batch.

        Args:
            calls: List of call tuples. Each call is one of:

                * ``(method, params)``
                * ``(method, params, formatter)``
                * ``(method, params, formatter, freshness)``

                ``freshness`` is a predicate
                ``freshness(result, required_tip, returned_tip) -> bool``.
            route: Routing hint: ``"pool"`` or ``"primary"``.

        Returns:
            A list aligned to ``calls``. Each element is either a result value
            (optionally formatted) or a `fastweb3.errors.RPCError`
            instance for the corresponding call.

        Raises:
            TransportError: For transport-level failures that cannot be
                recovered.
            AllEndpointsFailed: If all eligible endpoints failed.
            NoPrimaryEndpoint: If ``route="primary"`` is used without a
                configured primary.
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
                method, params = call
                fmt = None
                fresh = None
            elif len(call) == 3:
                method, params, fmt = call
                fresh = None
            elif len(call) == 4:
                method, params, fmt, fresh = call
            else:
                raise TypeError(
                    "Batch call must be (method, params), (method, params, formatter), "
                    "or (method, params, formatter, freshness)"
                )
            if not isinstance(params, (list, tuple)):
                raise TypeError("params must be a list or tuple")
            parsed.append(_BatchCall(method=method, params=params, formatter=fmt, freshness=fresh))

        ctx = MiddlewareContext(state={})
        parsed = self._run_middlewares_before(ctx, parsed)
        try:
            out = self._execute_batch(parsed, route=route) if parsed else []
        except Exception as exc:
            recovered = self._run_middlewares_on_exception(ctx, parsed, exc)
            if isinstance(recovered, Exception):
                raise recovered
            out = recovered
        out = self._run_middlewares_after(ctx, parsed, list(out))
        return list(out)
