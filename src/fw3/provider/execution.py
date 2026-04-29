"""Request execution logic for `fastweb3.provider.Provider`.

This mixin contains the core logic for single and batch execution, including:

* endpoint retries and cooldowns
* optional hedged requests
* optional "freshness" enforcement using block tip pinning
* optional RPC error retry for safe read methods
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Any, Sequence

from ..errors import AllEndpointsFailed, NoEndpoints, RPCError, TransportError
from ..formatters import to_int
from ..middleware import MiddlewareContext
from .rpc_error_retry import (
    _decide_rpc_error_retry,
    _normalize_rpc_error_message,
    _pin_calls_to_tip,
)
from .types import (
    _AttemptOutcome,
    _BatchAttemptOutcome,
    _BatchCall,
    _FreshnessUnmet,
    _RPCErrorObservation,
)


class ExecutionMixin:
    """Mixin that implements Provider.request and Provider.request_batch."""

    def _attempt(
        self,
        ep,
        method: str,
        params: list[Any] | tuple[Any, ...],
        formatter,
        freshness,
    ) -> tuple[Any | None, Exception | None]:
        outcome = self._attempt_raw(ep, method, params, formatter, freshness)
        return self._apply_outcome_single(ep, outcome)

    def _attempt_batch(
        self,
        ep,
        calls: list[_BatchCall],
    ) -> tuple[list[Any | RPCError] | None, Exception | None, int | None]:
        outcome = self._attempt_batch_raw(ep, calls)
        values, exc = self._apply_outcome_batch(ep, outcome)
        return values, exc, outcome.returned_tip

    def _attempt_raw(self, ep, method, params, formatter, freshness) -> _AttemptOutcome:
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

    def _attempt_batch_raw(self, ep, calls: list[_BatchCall]) -> _BatchAttemptOutcome:
        if not calls:
            return _BatchAttemptOutcome([], None, None)

        wants_freshness = any(c.freshness is not None for c in calls)
        required_tip = self._best_tip_snapshot()

        try:
            if self.desired_pool_size == 0:
                resp = ep.request_batch(*[(c.method, c.params, c.formatter) for c in calls])
                return _BatchAttemptOutcome(list(resp), None, None)

            resp_all = ep.request_batch(
                ("eth_blockNumber", (), to_int),
                *[(c.method, c.params, c.formatter) for c in calls],
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

    def _apply_outcome_single(
        self, ep, outcome: _AttemptOutcome
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
        self, ep, outcome: _BatchAttemptOutcome
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

        if isinstance(outcome.exc, RPCError):
            self._mark_failure(ep, outcome.exc)
            return None, outcome.exc

        if self.is_retryable_exc(outcome.exc):
            self._mark_failure(ep, outcome.exc)
            return None, outcome.exc

        raise outcome.exc

    def _maybe_retry_rpc_errors_in_batch(
        self,
        *,
        ep,
        calls: list[_BatchCall],
        results: list[Any | RPCError],
        returned_tip: int | None,
        route: str,
    ) -> list[Any | RPCError]:
        if route == "primary":
            return results

        history_by_index: dict[int, list[_RPCErrorObservation]] = {}

        def _observe(idx: int, obs_ep, err: RPCError) -> list[_RPCErrorObservation]:
            prev = history_by_index.get(idx, [])
            message = str(getattr(err, "message", err))
            obs = _RPCErrorObservation(
                endpoint_url=getattr(obs_ep, "url", str(obs_ep)),
                code=getattr(err, "code", None),
                message=message,
                normalized_message=_normalize_rpc_error_message(message),
            )
            history = [*prev, obs]
            history_by_index[idx] = history
            return history

        retry_indices: list[int] = []
        demote_initial = False

        for i, item in enumerate(results[: len(calls)]):
            if not isinstance(item, RPCError):
                continue
            history = _observe(i, ep, item)
            decision = _decide_rpc_error_retry(call=calls[i], history=history)
            if decision.retry:
                retry_indices.append(i)
                demote_initial = demote_initial or decision.demote_current_endpoint

        if not retry_indices:
            return results

        if demote_initial:
            self._mark_failure(ep, TransportError("RPCError selected for retry"))

        try:
            candidates = self._pool_candidates()
        except Exception:
            return results

        eligible = [e for e in self._eligible_endpoints(candidates) if e is not ep]
        if not eligible:
            return results

        ordered = sorted(eligible, key=self._last_tip, reverse=True)
        remaining = retry_indices

        for retry_ep in ordered:
            if not remaining:
                break

            retry_calls = [calls[i] for i in remaining]
            if returned_tip is not None:
                retry_calls = _pin_calls_to_tip(retry_calls, returned_tip)

            values2, exc2, _ = self._attempt_batch(retry_ep, retry_calls)
            if exc2 is not None:
                continue

            retried = list(values2 or [])
            for j, idx in enumerate(remaining):
                if j >= len(retried):
                    break
                results[idx] = retried[j]

            new_remaining: list[int] = []
            demote_retry_ep = False
            for j, idx in enumerate(remaining):
                if j >= len(retried):
                    continue
                item = results[idx]
                if not isinstance(item, RPCError):
                    continue
                history = _observe(idx, retry_ep, item)
                decision = _decide_rpc_error_retry(call=calls[idx], history=history)
                if decision.retry:
                    new_remaining.append(idx)
                demote_retry_ep = demote_retry_ep or decision.demote_current_endpoint

            if demote_retry_ep:
                self._mark_failure(retry_ep, TransportError("RPCError selected for retry"))

            remaining = new_remaining

        return results

    def _race_single(self, ep1, ep2, call: _BatchCall):
        q: queue.Queue[tuple[object, _AttemptOutcome]] = queue.Queue()

        def run(ep) -> None:
            try:
                outcome = self._attempt_raw(
                    ep, call.method, call.params, call.formatter, call.freshness
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

    def _race_batch(self, ep1, ep2, calls: list[_BatchCall]):
        q: queue.Queue[tuple[object, _BatchAttemptOutcome]] = queue.Queue()

        def run(ep) -> None:
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
            hedged_eps: tuple[object, object] | None = None

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
                if not isinstance(exc, _FreshnessUnmet):
                    exc_attempts += 1

            for idx, ep in enumerate(ordered):
                if hedged_eps is not None and idx < skip_n and ep in hedged_eps:
                    continue
                value, exc = self._attempt(
                    ep, call.method, call.params, call.formatter, call.freshness
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
            eps = self._eligible_endpoints(candidates)
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
