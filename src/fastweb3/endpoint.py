# src/fastweb3/endpoint.py
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

    Notes on batch semantics:
      - request() raises RPCError for JSON-RPC "error" responses.
      - request_batch() returns a list aligned to request order, where each element is either:
          * the (optionally formatted) result value, or
          * an RPCError instance for that specific call
        Batch-level failures (transport, malformed response shape, missing/duplicate ids, etc.)
        still raise immediately.
    """

    def __init__(
        self,
        target: str,
        *,
        transport: Optional[Transport] = None,
        config: EndpointConfig | None = None,
    ) -> None:
        self.target = target
        self.config = config or EndpointConfig()
        self.transport = transport or make_transport(target)
        self._counter = itertools.count(1)

    def _make_request(self, payload):
        try:
            return self.transport.send(payload)
        except TransportError:
            raise

    def _validate_response(self, resp):
        if not isinstance(resp, dict):
            raise RPCMalformedResponse(
                f"Response must be a JSON object, got: {type(resp).__name__}"
            )
        if resp.get("jsonrpc") != "2.0":
            raise RPCMalformedResponse(f"Missing/invalid jsonrpc field: {resp!r}")
        if "id" not in resp:
            raise RPCMalformedResponse(f"Missing id field: {resp!r}")

    def _build_rpc_error(self, resp):
        err = resp["error"]
        if isinstance(err, dict):
            details = RPCErrorDetails(
                code=err.get("code"),
                message=err.get("message"),
                data=err.get("data"),
            )
        else:
            details = RPCErrorDetails(code=None, message=str(err), data=None)
        return RPCError(details)

    def close(self) -> None:
        self.transport.close()

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        formatter: Formatter | None = None,
    ) -> Any:
        request_id = next(self._counter)
        payload: Mapping[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": list(params),
        }

        resp = self._make_request(payload)
        self._validate_response(resp)

        if resp.get("error") is not None:
            raise self._build_rpc_error(resp)

        if "result" not in resp:
            raise RPCMalformedResponse(f"Missing result field: {resp!r}")

        result = resp["result"]
        if formatter is not None:
            result = formatter(result)
        return result

    def request_batch(
        self,
        *calls: tuple[str, Sequence[Any]]
        | tuple[str, Sequence[Any], Formatter]
        | tuple[str, Sequence[Any], None],
    ) -> list[Any | RPCError]:
        """
        Execute a JSON-RPC batch request.

        Each call is either:
          - (method, params)
          - (method, params, formatter)

        Return value:
          A list aligned to the call order. Each element is either:
            - the (optionally formatted) result value, or
            - an RPCError instance if that specific call returned a JSON-RPC error object.

        Raises:
          - TransportError for network/transport failures
          - RPCMalformedResponse for invalid/malformed batch responses
        """
        if not calls:
            return []

        # Allocate ids and build payload
        ids: list[int] = []
        payload: list[dict[str, Any]] = []
        formatters: list[Formatter | None] = []

        for call in calls:
            if not isinstance(call, tuple):
                raise TypeError(f"Batch call must be a tuple, got: {type(call).__name__}")

            if len(call) == 2:
                method, params = call  # type: ignore[misc]
                fmt: Formatter | None = None
            elif len(call) == 3:
                method, params, fmt = call  # type: ignore[misc]
            else:
                raise TypeError(
                    "Batch call must be (method, params) or (method, params, formatter)"
                )

            request_id = next(self._counter)
            ids.append(request_id)
            formatters.append(fmt)
            payload.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": list(params),
                }
            )

        resp = self._make_request(payload)

        if not isinstance(resp, list):
            raise RPCMalformedResponse(f"Batch response must be a list, got: {type(resp).__name__}")

        by_id: dict[int, dict[str, Any]] = {}
        for item in resp:
            self._validate_response(item)

            _id = item["id"]
            if not isinstance(_id, int):
                raise RPCMalformedResponse(f"Non-int id in batch item: {item!r}")
            if _id in by_id:
                raise RPCMalformedResponse(f"Duplicate id in batch response: {_id}")
            by_id[_id] = item

        # Ensure all requested ids exist
        for _id in ids:
            if _id not in by_id:
                raise RPCMalformedResponse(f"Missing id {_id} in batch response")

        out: list[Any | RPCError] = []
        for i, _id in enumerate(ids):
            item = by_id[_id]

            # Per-call JSON-RPC error: return RPCError instance in-position.
            if item.get("error") is not None:
                err = self._build_rpc_error(item)
                out.append(err)
                continue

            if "result" not in item:
                raise RPCMalformedResponse(f"Missing result in batch item: {item!r}")

            val = item["result"]
            fmt = formatters[i]
            if fmt is not None:
                val = fmt(val)
            out.append(val)

        return out
