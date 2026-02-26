from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

from .errors import RPCError, RPCErrorDetails, RPCMalformedResponse, TransportError
from .transport import Transport, make_transport

Formatter = Callable[[Any], Any]


@dataclass(frozen=True)
class EndpointConfig:
    # placeholder for later: per-endpoint timeouts, tags, weights, etc.
    pass


class Endpoint:
    """
    JSON-RPC semantics for a single endpoint target.

    Owns:
      - request id counter
      - JSON-RPC envelope
      - response validation / error parsing
    Delegates wire I/O to Transport (HTTP / WSS / IPC).
    """

    def __init__(
        self,
        url: str,
        *,
        transport: Optional[Transport] = None,
        config: EndpointConfig | None = None,
    ) -> None:
        # keep name `url` for backwards compatibility, but it's really a "target"
        self.url = url
        self.config = config or EndpointConfig()
        self.transport = transport or make_transport(url)
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
        except TransportError:
            raise

        if not isinstance(resp, dict):
            raise RPCMalformedResponse(
                f"Single response must be a JSON object, got: {type(resp).__name__}"
            )

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

    def request_batch(
        self,
        calls: Sequence[tuple[str, list[Any] | tuple[Any, ...]]],
        *,
        formatters: Mapping[int, Formatter] | None = None,  # keyed by index in calls
    ) -> list[Any]:
        # allocate ids
        ids: list[int] = []
        payload: list[dict[str, Any]] = []
        for method, params in calls:
            request_id = next(self._counter)
            ids.append(request_id)
            payload.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": list(params),
                }
            )

        try:
            resp = self.transport.send(payload)
        except TransportError:
            raise

        if not isinstance(resp, list):
            raise RPCMalformedResponse(f"Batch response must be a list, got: {type(resp).__name__}")

        by_id: dict[int, dict[str, Any]] = {}
        for item in resp:
            if not isinstance(item, dict):
                raise RPCMalformedResponse(f"Batch item must be dict: {item!r}")
            if item.get("jsonrpc") != "2.0":
                raise RPCMalformedResponse(f"Invalid jsonrpc in batch item: {item!r}")
            if "id" not in item:
                raise RPCMalformedResponse(f"Missing id in batch item: {item!r}")

            _id = item["id"]
            if not isinstance(_id, int):
                raise RPCMalformedResponse(f"Non-int id in batch item: {item!r}")
            if _id in by_id:
                raise RPCMalformedResponse(f"Duplicate id in batch response: {_id}")
            by_id[_id] = item

        # ensure all requested ids exist
        for _id in ids:
            if _id not in by_id:
                raise RPCMalformedResponse(f"Missing id {_id} in batch response")

        out: list[Any] = []
        for i, _id in enumerate(ids):
            item = by_id[_id]

            if item.get("error") is not None:
                err = item["error"]
                if isinstance(err, dict):
                    details = RPCErrorDetails(
                        code=err.get("code"),
                        message=err.get("message"),
                        data=err.get("data"),
                    )
                else:
                    details = RPCErrorDetails(code=None, message=str(err), data=None)
                raise RPCError(details)

            if "result" not in item:
                raise RPCMalformedResponse(f"Missing result in batch item: {item!r}")

            val = item["result"]
            if formatters is not None and i in formatters:
                val = formatters[i](val)
            out.append(val)

        return out
