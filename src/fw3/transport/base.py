"""Transport protocol definitions.

Transports are responsible for sending JSON-RPC payloads and returning decoded
JSON responses.
"""

from __future__ import annotations

from typing import Any, Mapping, Protocol, TypeAlias

JSONObj: TypeAlias = Mapping[str, Any]
JSONPayload: TypeAlias = JSONObj | list[JSONObj]
JSONResp: TypeAlias = dict[str, Any] | list[dict[str, Any]]


class Transport(Protocol):
    """Wire-level transport interface.

    The current API is synchronous and supports both single-request and batch
    payloads.
    """

    def send(self, payload: JSONPayload) -> JSONResp:
        """Send a JSON-RPC payload.

        Args:
            payload: JSON-RPC payload (single object or batch list).

        Returns:
            Decoded JSON response.

        Raises:
            Exception: Transport implementations typically raise
                `fastweb3.errors.TransportError` for wire-level failures.
        """
        ...

    def close(self) -> None:
        """Close any resources owned by the transport."""
        ...
