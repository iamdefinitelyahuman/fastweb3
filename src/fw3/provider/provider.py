"""Provider implementation.

The provider layer powers `fastweb3.web3.web3.Web3`. It coordinates:

* endpoint selection (internal endpoints, primary endpoint, and the public pool)
* request execution with retry/hedging
* middleware orchestration

Most users should use `fastweb3.web3.web3.Web3` instead of creating a
provider directly.
"""

from __future__ import annotations

import threading
from typing import Callable, Sequence

from ..errors import TransportError
from ..middleware import _apply_default_middlewares
from .attempts import AttemptMixin
from .endpoint_selection import EndpointSelectionMixin
from .execution import ExecutionMixin
from .hedging import HedgingMixin
from .middleware import MiddlewareMixin
from .pool import PoolManager
from .rpc_error_handling import RPCErrorHandlingMixin
from .types import RetryPolicy


def _default_is_retryable_exc(exc: Exception) -> bool:
    return isinstance(exc, TransportError)


class Provider(
    MiddlewareMixin,
    EndpointSelectionMixin,
    AttemptMixin,
    HedgingMixin,
    RPCErrorHandlingMixin,
    ExecutionMixin,
):
    """Facade over endpoint selection, execution, and middleware orchestration.

    This is an advanced interface. In typical usage, you should construct and
    interact with `fastweb3.web3.web3.Web3` instead.
    """

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
        """Create a provider.

        Args:
            internal_endpoints: Optional sequence of endpoint targets to use as
                the internal pool. These are deduplicated and merged with any
                pool-manager URLs.
            pool_manager: Optional pool manager used to discover public RPC
                endpoints for the configured chain.
            desired_pool_size: Desired total pool size when using a pool
                manager. The provider will attempt to fill the pool up to this
                size (including internal endpoints).
            retry_policy_pool: Retry policy for pool requests.
            is_retryable_exc: Predicate used to classify exceptions as
                retryable.
            hedge_after_seconds: If set, issue a "hedged" second request to a
                different endpoint after this delay when the first request has
                not completed.
            hedge_slow_cooldown_seconds: Cooldown applied to an endpoint that
                loses a hedged race.

        Raises:
            ValueError: If ``hedge_after_seconds`` is set below the minimum.
        """
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
