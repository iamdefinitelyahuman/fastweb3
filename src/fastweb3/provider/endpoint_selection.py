"""Endpoint selection and endpoint lifecycle management.

This mixin tracks internal endpoints, an optional primary endpoint, and an
optional pool-manager-derived set of public endpoints.
"""

from __future__ import annotations

import time

from ..endpoint import Endpoint
from ..errors import NoEndpoints, NoPrimaryEndpoint
from ..utils import normalize_target
from .types import _EndpointState


class EndpointSelectionMixin:
    """Mixin implementing endpoint management for `fastweb3.provider.Provider`."""

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

    def set_primary(self, target: str) -> None:
        """Set the primary endpoint.

        The primary endpoint is used for node-local/stateful methods or when
        explicitly routing requests to ``route="primary"``.

        Args:
            target: Endpoint target string.
        """
        ep = self._get_or_create_endpoint(target)
        with self._lock:
            self._primary = ep

    def clear_primary(self) -> None:
        """Clear the configured primary endpoint."""
        with self._lock:
            self._primary = None

    def primary_endpoint(self) -> str | None:
        """Return the primary endpoint target, if configured."""
        with self._lock:
            return self._primary.target if self._primary is not None else None

    def has_primary(self) -> bool:
        """Return ``True`` if a primary endpoint is configured."""
        with self._lock:
            return self._primary is not None

    def _get_primary(self) -> Endpoint:
        with self._lock:
            if self._primary is None:
                raise NoPrimaryEndpoint("Primary endpoint is unset")
            return self._primary

    def add_endpoint(self, target: str, *, priority: bool = False) -> None:
        """Add an internal endpoint target.

        Args:
            target: Endpoint target string.
            priority: If ``True``, insert at the front of the internal list.
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
        """Remove an internal endpoint target.

        Args:
            target: Endpoint target string.
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
        """Return a snapshot of configured internal endpoint targets."""
        with self._lock:
            return list(self._internal_targets)

    def close(self) -> None:
        """Close all managed endpoints and clear internal state."""
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
        _ = exc
        now = time.time()
        with self._lock:
            st = self._state.get(ep)
            if st is None:
                return
            st.failures += 1
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
                st.tip_cooldown_until = max(st.tip_cooldown_until, now + 2.0)

    def _eligible_endpoints(self, eps: list[Endpoint]) -> list[Endpoint]:
        now = time.time()
        return [ep for ep in eps if not self._is_cooldown_active(ep, now)]

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
