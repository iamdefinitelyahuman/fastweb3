# src/fastweb3/transport/http.py
"""HTTP transport implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Union

import httpx

from ..errors import TransportError

JSONObj = Mapping[str, Any]
JSONPayload = Union[JSONObj, List[JSONObj]]
JSONResp = Union[dict[str, Any], list[dict[str, Any]]]


@dataclass(frozen=True)
class HTTPTransportConfig:
    """Configuration for `HTTPTransport`.

    Attributes:
        timeout: Default request timeout.
        connect_timeout: Connection establishment timeout.
        read_timeout: Read timeout.
        write_timeout: Write timeout.
        pool_timeout: Connection pool acquisition timeout.
        max_connections: Maximum number of concurrent connections.
        max_keepalive_connections: Maximum number of keep-alive connections.
        keepalive_expiry: Keep-alive expiry time.
        headers: Optional headers to include in requests.
    """

    timeout: float = 20.0
    connect_timeout: float = 5.0
    read_timeout: float = 20.0
    write_timeout: float = 20.0
    pool_timeout: float = 5.0

    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_expiry: float = 30.0

    headers: Optional[Mapping[str, str]] = None


class HTTPTransport:
    """Synchronous HTTP transport for JSON-RPC."""

    def __init__(
        self,
        url: str,
        *,
        config: HTTPTransportConfig | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        """Create an HTTP transport.

        Args:
            url: HTTP(S) URL.
            config: Optional transport configuration.
            client: Optional pre-configured `httpx.Client`. If provided,
                the caller owns the client lifecycle.
        """
        self.url = url
        self.config = config or HTTPTransportConfig()

        if client is not None:
            self._client = client
            self._owns_client = False
            return

        timeout = httpx.Timeout(
            timeout=self.config.timeout,
            connect=self.config.connect_timeout,
            read=self.config.read_timeout,
            write=self.config.write_timeout,
            pool=self.config.pool_timeout,
        )
        limits = httpx.Limits(
            max_connections=self.config.max_connections,
            max_keepalive_connections=self.config.max_keepalive_connections,
            keepalive_expiry=self.config.keepalive_expiry,
        )
        self._client = httpx.Client(
            timeout=timeout,
            limits=limits,
            headers=dict(self.config.headers or {}),
        )
        self._owns_client = True

    def close(self) -> None:
        """Close the underlying `httpx.Client` if owned."""
        if self._owns_client:
            self._client.close()

    def send(self, payload: JSONPayload) -> JSONResp:
        """Send a JSON-RPC payload and return the decoded JSON response.

        Args:
            payload: JSON-RPC payload (single object or batch list).

        Returns:
            Decoded JSON response.

        Raises:
            TransportError: For network errors and non-2xx HTTP responses.
            TypeError: If the response JSON is not an object or list of objects.
        """
        try:
            resp = self._client.post(self.url, json=payload)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # Preserve status for cooldown logic (429 etc.)
            raise TransportError(str(exc), status_code=exc.response.status_code) from exc
        except httpx.RequestError as exc:
            # Connection / protocol / timeout errors (includes RemoteProtocolError)
            raise TransportError(str(exc)) from exc
        data = resp.json()
        if not isinstance(data, (dict, list)):
            raise TypeError(f"Expected JSON object or array, got {type(data).__name__}")
        if isinstance(data, list) and not all(isinstance(x, dict) for x in data):
            raise TypeError("Expected JSON array of objects")
        return data
