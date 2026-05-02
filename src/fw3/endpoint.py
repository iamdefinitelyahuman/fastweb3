# src/fastweb3/endpoint.py
"""Endpoint-level JSON-RPC request/response handling.

An `fastweb3.endpoint.Endpoint` encapsulates JSON-RPC semantics for a
single RPC target (URL or IPC path), including:

* request ID generation
* JSON-RPC envelope construction
* response validation and JSON-RPC error parsing

Wire I/O is delegated to a `fastweb3.transport.base.Transport`.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

from .errors import RPCError, RPCErrorDetails, RPCMalformedResponse
from .transport import Transport, make_transport

Formatter = Callable[[Any], Any]


@dataclass(frozen=True)
class EndpointConfig:
    """Configuration for an `Endpoint`.

    This is currently a placeholder for future per-endpoint knobs (timeouts,
    weights, tags, etc.).
    """

    # placeholder for later: per-endpoint timeouts, tags, weights, etc.
    pass


class Endpoint:
    """JSON-RPC semantics for a single endpoint target.

    An endpoint is responsible for JSON-RPC request IDs, envelope construction,
    and response validation. It delegates wire I/O to a transport (HTTP/WSS/IPC).

    Batch semantics:
        * `request()` raises `fastweb3.errors.RPCError` when a JSON-RPC
          response includes an ``error`` object.
        * `request_batch()` returns a list aligned to the request order.
          Each element is either a result value or a `fastweb3.errors.RPCError`
          instance for that particular call.

          Batch-level failures (transport errors, malformed response shape,
          missing/duplicate IDs, etc.) raise immediately.
    """

    def __init__(
        self,
        target: str,
        *,
        transport: Optional[Transport] = None,
        config: EndpointConfig | None = None,
    ) -> None:
        """Create an endpoint.

        Args:
            target: Endpoint target string. Typically an HTTP(S) or WS(S) URL,
                or an IPC target.
            transport: Optional transport instance. If omitted, a transport is
                created based on ``target``.
            config: Optional endpoint configuration.
        """
        self.target = target
        self.config = config or EndpointConfig()
        self.transport = transport or make_transport(target)
        self._counter = itertools.count(1)

    def _build_payload(self, request_id: int, method: str, params: Sequence[Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": list(params)}

    def _make_request(self, payload):
        return self.transport.send(payload)

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
        return RPCError(details, endpoint=self.target)

    def close(self) -> None:
        """Close the underlying transport."""
        self.transport.close()

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        formatter: Formatter | None = None,
    ) -> Any:
        """Execute a single JSON-RPC request.

        Args:
            method: JSON-RPC method name (e.g. ``"eth_getBalance"``).
            params: JSON-RPC params.
            formatter: Optional function applied to the raw ``result``.

        Returns:
            The (optionally formatted) JSON-RPC ``result``.

        Raises:
            RPCError: If the JSON-RPC response includes an ``error`` object.
            RPCMalformedResponse: If the response shape does not match the
                JSON-RPC 2.0 expectations.
            TransportError: For network/transport failures.
        """
        request_id = next(self._counter)
        payload: Mapping[str, Any] = self._build_payload(request_id, method, params)

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
        """Execute a JSON-RPC batch request.

        Each call is one of:

        * ``(method, params)``
        * ``(method, params, formatter)``

        Returns:
            A list aligned to the call order. Each element is either a result
            value (optionally formatted) or a `fastweb3.errors.RPCError`
            instance if that specific call returned a JSON-RPC error.

        Raises:
            RPCMalformedResponse: If the batch response shape is invalid.
            TransportError: For network/transport failures.
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
            payload.append(self._build_payload(request_id, method, params))

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
