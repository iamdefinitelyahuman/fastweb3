from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import httpx


@dataclass(frozen=True)
class HTTPTransportConfig:
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
    """
    Sync HTTP transport for JSON-RPC.
    """

    def __init__(
        self,
        url: str,
        *,
        config: HTTPTransportConfig | None = None,
        client: httpx.Client | None = None,
    ) -> None:
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
        if self._owns_client:
            self._client.close()

    def send(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        resp = self._client.post(self.url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise TypeError(f"Expected JSON object (dict), got {type(data).__name__}")
        return data
