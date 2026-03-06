from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..endpoint import Formatter
from ..errors import RPCError, TransportError


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
    """Internal instruction for how to treat a single RPCError result."""

    retry: bool = False
    demote_current_endpoint: bool = False


@dataclass(frozen=True)
class _RPCErrorObservation:
    """Observation of an RPCError returned by an endpoint for a given call."""

    endpoint_url: str
    code: int | None
    message: str
    normalized_message: str
