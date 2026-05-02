"""Internal provider types.

This module contains small dataclasses used by the provider implementation.
Only `RetryPolicy` is considered part of the advanced public API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..endpoint import Formatter
from ..errors import RPCError, TransportError


@dataclass(frozen=True)
class RetryPolicy:
    """Retry configuration for pool requests.

    Attributes:
        max_attempts: Maximum number of attempts across distinct endpoints.
        backoff_seconds: Fixed backoff applied between attempts.
    """

    max_attempts: int = 3
    backoff_seconds: float = 0.05


@dataclass(frozen=True)
class _BatchCall:
    """Internal canonical representation of a single call in a batch."""

    method: str
    params: list[Any] | tuple[Any, ...]
    formatter: Formatter | None = None
    freshness: Callable[[Any, int, int], bool] | None = None


@dataclass
class _EndpointState:
    """Internal per-endpoint health and cooldown state."""

    # Cooldown due to request/transport failures (exponential-ish backoff)
    error_cooldown_until: float = 0.0
    failures: int = 0

    # Cooldown due to stale tip observation
    tip_cooldown_until: float = 0.0

    # Cooldown due to being the "loser" in a hedged race
    slow_cooldown_until: float = 0.0

    # Last tip observed from this endpoint target (LB backends may vary)
    last_tip: int | None = None

    # RPC methods this endpoint has explicitly rejected as unavailable.
    unsupported_methods: set[str] | None = None


class _FreshnessUnmet(TransportError):
    """Internal sentinel for 'response received, but endpoint tip is too stale for this call'."""


@dataclass(frozen=True)
class _AttemptOutcome:
    """Internal result container for a single attempt."""

    value: Any | None
    exc: Exception | None
    returned_tip: int | None


@dataclass(frozen=True)
class _BatchAttemptOutcome:
    """Internal result container for a batch attempt."""

    values: list[Any | RPCError] | None
    exc: Exception | None
    returned_tip: int | None


@dataclass(frozen=True)
class _RPCErrorRetryDecision:
    """Internal instruction for how to treat a single RPCError result."""

    retry: bool = False
    demote_current_endpoint: bool = False
    unsupported_method: bool = False


@dataclass(frozen=True)
class _RPCErrorObservation:
    """Observation of an RPCError returned by an endpoint for a given call."""

    endpoint_url: str
    code: int | None
    message: str
    normalized_message: str
