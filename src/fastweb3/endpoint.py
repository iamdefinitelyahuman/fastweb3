from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

import httpx

from .errors import RPCError, RPCErrorDetails, RPCMalformedResponse, TransportError
from .transport import HTTPTransport, Transport

Formatter = Callable[[Any], Any]


@dataclass(frozen=True)
class EndpointConfig:
    # placeholder for later: per-endpoint timeouts, tags, weights, etc.
    pass


class Endpoint:
    """
    JSON-RPC semantics for a single endpoint URL.

    Owns:
      - request id counter
      - JSON-RPC envelope
      - response validation / error parsing
    Delegates wire I/O to Transport (HTTP now; WS/IPC later).
    """

    def __init__(
        self,
        url: str,
        *,
        transport: Optional[Transport] = None,
        config: EndpointConfig | None = None,
    ) -> None:
        self.url = url
        self.config = config or EndpointConfig()
        self.transport = transport or HTTPTransport(url)
        self._counter = itertools.count(1)

    def close(self) -> None:
        self.transport.close()

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        formatter: Formatter | None = None,
    ) -> Any:
        request_id = next(self._counter)
        payload: Mapping[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": list(params),
        }

        try:
            resp = self.transport.send(payload)
        except httpx.HTTPError as exc:
            # If the underlying transport is HTTPTransport, it may throw httpx errors.
            # Normalize anyway. Other transports should raise TransportError directly.
            raise TransportError(str(exc)) from exc
        except TransportError:
            raise

        if resp.get("jsonrpc") != "2.0":
            raise RPCMalformedResponse(f"Missing/invalid jsonrpc field: {resp!r}")
        if "id" not in resp:
            raise RPCMalformedResponse(f"Missing id field: {resp!r}")

        if "error" in resp and resp["error"] is not None:
            err = resp["error"]
            if isinstance(err, dict):
                details = RPCErrorDetails(
                    code=err.get("code"),
                    message=err.get("message"),
                    data=err.get("data"),
                )
            else:
                details = RPCErrorDetails(code=None, message=str(err), data=None)
            raise RPCError(details)

        if "result" not in resp:
            raise RPCMalformedResponse(f"Missing result field: {resp!r}")

        result = resp["result"]
        if formatter is not None:
            result = formatter(result)
        return result

    # ---- scaffolding for later batching (intentionally inert for now) ----

    def batch(self):
        """
        Placeholder for future per-thread batching on this endpoint.
        """
        raise NotImplementedError("Batching not implemented yet")
