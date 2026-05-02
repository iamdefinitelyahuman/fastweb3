"""Endpoint attempt helpers for `fastweb3.provider.Provider`."""

from __future__ import annotations

from typing import Any

from ..errors import RPCError, TransportError
from ..formatters import to_int
from .types import _AttemptOutcome, _BatchAttemptOutcome, _BatchCall, _FreshnessUnmet


class AttemptMixin:
    """Mixin that executes raw endpoint attempts and applies their outcomes."""

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
