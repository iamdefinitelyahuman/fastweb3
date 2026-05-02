"""Error types raised by fastweb3.

fastweb3 distinguishes between:

* **Transport errors**: network/HTTP/IPC/WebSocket issues and other
  wire-level failures.
* **RPC errors**: JSON-RPC responses that include an ``error`` object.
* **Client validation errors**: inputs rejected before an RPC call is made.

Only a small subset of these errors are exported from the package root.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RPCErrorDetails:
    """Structured information extracted from a JSON-RPC error object.

    Attributes:
        code: Error code if provided by the server.
        message: Human-readable error message if provided.
        data: Optional additional error data.
    """

    code: int | None
    message: str | None
    data: Any = None


class FastWeb3Error(Exception):
    """Base error for fastweb3."""


class TransportError(FastWeb3Error):
    """Network/HTTP-level error (timeouts, connection failures, non-2xx, etc.)."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class RPCError(FastWeb3Error):
    """JSON-RPC-level error (error object in response)."""

    def __init__(self, details: RPCErrorDetails, *, endpoint: str | None = None) -> None:
        super().__init__(f"RPCError {details.code}: {details.message}")
        self.details = details
        self.endpoint = endpoint


class RPCMalformedResponse(FastWeb3Error):
    """Response didn't match expected JSON-RPC shape."""


class NoEndpoints(FastWeb3Error):
    """Provider has no endpoints configured."""


class NoPrimaryEndpoint(FastWeb3Error):
    """Primary endpoint is required for this call but is not set."""


class AllEndpointsFailed(FastWeb3Error):
    """Provider tried all endpoints and none succeeded."""

    def __init__(self, last_exc: Exception | None = None) -> None:
        super().__init__("All endpoints failed")
        self.last_exc = last_exc


class ValidationError(FastWeb3Error):
    """Client-side validation error (inputs are malformed before RPC)."""
