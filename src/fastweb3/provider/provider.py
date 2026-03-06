from __future__ import annotations

import threading
from typing import Callable, Sequence

from ..errors import TransportError
from ..middleware import _apply_default_middlewares
from ..rpc_pool import PoolManager
from .endpoint_selection import EndpointSelectionMixin
from .execution import ExecutionMixin
from .middleware import MiddlewareMixin
from .types import RetryPolicy


def _default_is_retryable_exc(exc: Exception) -> bool:
    return isinstance(exc, TransportError)


class Provider(MiddlewareMixin, EndpointSelectionMixin, ExecutionMixin):
    """Facade over endpoint selection, execution, and middleware orchestration."""

    _FRESHNESS_WAIT_CAP_SECONDS: float = 5.0
    _HEDGE_MIN_DELAY_SECONDS: float = 0.05

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
        self._middlewares: list[object] = []

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

        self._internal_targets: list[str] = []
        self._internal_seen: set[str] = set()
        self._eps_by_target: dict[str, object] = {}
        self._state: dict[object, object] = {}
        self._primary = None
        self._best_tip: int | None = None

        for t in internal_endpoints_list:
            self.add_endpoint(t, priority=False)

        _apply_default_middlewares(self)
