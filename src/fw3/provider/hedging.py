"""Hedged request helpers for `fastweb3.provider.Provider`."""

from __future__ import annotations

import queue
import threading

from .types import _AttemptOutcome, _BatchAttemptOutcome, _BatchCall


class HedgingMixin:
    """Mixin that races duplicate attempts across endpoints."""

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
