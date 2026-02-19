from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RPCErrorDetails:
    code: int | None
    message: str | None
    data: Any = None


class FastWeb3Error(Exception):
    """Base error for fastweb3."""


class TransportError(FastWeb3Error):
    """Network/HTTP-level error (timeouts, connection failures, non-2xx, etc.)."""


class RPCError(FastWeb3Error):
    """JSON-RPC-level error (error object in response)."""

    def __init__(self, details: RPCErrorDetails) -> None:
        super().__init__(f"RPCError {details.code}: {details.message}")
        self.details = details


class RPCMalformedResponse(FastWeb3Error):
    """Response didn't match expected JSON-RPC shape."""


class NoEndpoints(FastWeb3Error):
    """Provider has no endpoints configured."""


class AllEndpointsFailed(FastWeb3Error):
    """Provider tried all endpoints and none succeeded."""

    def __init__(self, last_exc: Exception | None = None) -> None:
        super().__init__("All endpoints failed")
        self.last_exc = last_exc
