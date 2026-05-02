"""RPC-error retry helpers for `fastweb3.provider.Provider`."""

from __future__ import annotations

from typing import Any

from ..errors import RPCError, TransportError
from .rpc_error_retry import (
    _decide_rpc_error_retry,
    _normalize_rpc_error_message,
    _pin_calls_to_tip,
)
from .types import _AttemptOutcome, _BatchCall, _RPCErrorObservation


class RPCErrorHandlingMixin:
    """Mixin that observes retryable RPC errors and applies related side effects."""

    def _observe_rpc_error(
        self,
        history: list[_RPCErrorObservation],
        ep,
        err: RPCError,
    ) -> list[_RPCErrorObservation]:
        message = err.details.message or ""
        return [
            *history,
            _RPCErrorObservation(
                endpoint_url=getattr(ep, "target", str(ep)),
                code=err.details.code,
                message=message,
                normalized_message=_normalize_rpc_error_message(message),
            ),
        ]

    def _maybe_retry_rpc_error_single(
        self,
        *,
        ep,
        call: _BatchCall,
        err: RPCError,
        history: list[_RPCErrorObservation],
    ) -> tuple[bool, list[_RPCErrorObservation]]:
        history = self._observe_rpc_error(history, ep, err)
        decision = _decide_rpc_error_retry(call=call, history=history)
        if decision.unsupported_method:
            self._mark_method_unsupported(ep, call.method)
        if decision.demote_current_endpoint:
            self._mark_failure(ep, TransportError("RPCError selected for retry"))
        return decision.retry, history

    def _attempt_with_rpc_error_retry(
        self,
        ep,
        call: _BatchCall,
        history: list[_RPCErrorObservation],
    ) -> tuple[Any | None, Exception | None, list[_RPCErrorObservation]]:
        try:
            value, exc = self._attempt(ep, call.method, call.params, call.formatter, call.freshness)
            return value, exc, history
        except RPCError as err:
            retry, history = self._maybe_retry_rpc_error_single(
                ep=ep, call=call, err=err, history=history
            )
            if retry:
                return None, err, history
            raise

    def _apply_outcome_single_with_rpc_error_retry(
        self,
        ep,
        call: _BatchCall,
        outcome: _AttemptOutcome,
        history: list[_RPCErrorObservation],
    ) -> tuple[Any | None, Exception | None, list[_RPCErrorObservation]]:
        try:
            value, exc = self._apply_outcome_single(ep, outcome)
            return value, exc, history
        except RPCError as err:
            retry, history = self._maybe_retry_rpc_error_single(
                ep=ep, call=call, err=err, history=history
            )
            if retry:
                return None, err, history
            raise

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
            history = self._observe_rpc_error(history_by_index.get(idx, []), obs_ep, err)
            history_by_index[idx] = history
            return history

        retry_indices: list[int] = []
        demote_initial = False

        for i, item in enumerate(results[: len(calls)]):
            if not isinstance(item, RPCError):
                continue
            history = _observe(i, ep, item)
            decision = _decide_rpc_error_retry(call=calls[i], history=history)
            if decision.unsupported_method:
                self._mark_method_unsupported(ep, calls[i].method)
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

        eligible = [
            e
            for e in self._eligible_endpoints(candidates, {calls[i].method for i in retry_indices})
            if e is not ep
        ]
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
                if decision.unsupported_method:
                    self._mark_method_unsupported(retry_ep, calls[idx].method)
                if decision.retry:
                    new_remaining.append(idx)
                demote_retry_ep = demote_retry_ep or decision.demote_current_endpoint

            if demote_retry_ep:
                self._mark_failure(retry_ep, TransportError("RPCError selected for retry"))

            remaining = new_remaining

        return results
