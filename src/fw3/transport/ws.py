"""WebSocket transport implementation."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from ..errors import TransportError
from .base import JSONObj, JSONPayload, JSONResp

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover
    websocket = None  # type: ignore


@dataclass(frozen=True)
class WSSTransportConfig:
    """Configuration for `WSSTransport`.

    Attributes:
        connect_timeout: Timeout for establishing the WebSocket connection.
        recv_timeout: Timeout for receiving a response.
        reconnect_backoff_s: Backoff when the receiver detects disconnect.
        headers: Optional headers for the WebSocket handshake.
    """

    connect_timeout: float = 5.0
    recv_timeout: float = 20.0

    # Reconnect backoff when receiver detects disconnect
    reconnect_backoff_s: float = 0.25

    # Optional headers for the WS handshake
    headers: Optional[Mapping[str, str]] = None


class _Waiter:
    """Waiter for a single in-flight JSON-RPC request/response."""

    __slots__ = ("cv", "done", "result", "exc")

    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.done = False
        self.result: Optional[dict[str, Any]] = None
        self.exc: Optional[Exception] = None

    def set_result(self, r: dict[str, Any]) -> None:
        """Resolve the waiter with a response object."""
        with self.cv:
            self.result = r
            self.done = True
            self.cv.notify_all()

    def set_exc(self, e: Exception) -> None:
        """Resolve the waiter with an exception."""
        with self.cv:
            self.exc = e
            self.done = True
            self.cv.notify_all()

    def wait(self, timeout: float) -> dict[str, Any]:
        """Wait for the response or raise on timeout/error."""
        end = time.time() + timeout
        with self.cv:
            while not self.done:
                remaining = end - time.time()
                if remaining <= 0:
                    raise TransportError("WSS recv timeout waiting for response")
                self.cv.wait(timeout=remaining)
            if self.exc is not None:
                raise self.exc
            assert self.result is not None
            return self.result


class WSSTransport:
    """
    Sync WebSocket transport for JSON-RPC (request/response only).

    Requires: websocket-client
    """

    def __init__(self, url: str, *, config: WSSTransportConfig | None = None) -> None:
        """Create a WebSocket transport.

        Args:
            url: WebSocket URL.
            config: Optional transport configuration.

        Raises:
            ImportError: If ``websocket-client`` is not installed.
        """
        if websocket is None:  # pragma: no cover
            raise ImportError(
                "WSSTransport requires 'websocket-client' (pip install websocket-client)"
            )

        self.url = url
        self.config = config or WSSTransportConfig()

        self._lock = threading.Lock()
        self._ws: Optional["websocket.WebSocket"] = None
        self._rx_thread: Optional[threading.Thread] = None
        self._closed = False

        self._inflight: dict[Any, _Waiter] = {}
        self._inflight_lock = threading.Lock()

    def close(self) -> None:
        """Close the transport and fail any in-flight requests."""
        self._closed = True
        with self._lock:
            if self._ws is not None:
                try:
                    self._ws.close()
                finally:
                    self._ws = None

        # Fail any in-flight waiters
        self._fail_all_inflight(TransportError("WSS transport closed"))

    def send(self, payload: JSONPayload) -> JSONResp:
        """Send a JSON-RPC payload and wait for the correlated response(s).

        Args:
            payload: JSON-RPC payload (single object or batch list). The payload
                must include ``id`` fields so responses can be correlated.

        Returns:
            Decoded JSON response.

        Raises:
            TransportError: For connection issues, timeouts, and correlation
                errors.
            ValueError: If a batch payload is empty.
        """
        # Normalize payload into (single|batch) and list of ids to wait for
        if isinstance(payload, list):
            if not payload:
                raise ValueError("Empty batch payload")
            ids = [self._get_id(p) for p in payload]
            waiter_map = {i: _Waiter() for i in ids}
        else:
            ids = [self._get_id(payload)]
            waiter_map = {ids[0]: _Waiter()}

        # Register inflight BEFORE send
        with self._inflight_lock:
            for i, w in waiter_map.items():
                if i in self._inflight:
                    raise TransportError(f"Duplicate in-flight request id: {i!r}")
                self._inflight[i] = w

        try:
            self._ensure_connected()
            msg = json.dumps(payload, separators=(",", ":"))
            with self._lock:
                # ws send must be serialized
                assert self._ws is not None
                self._ws.send(msg)
        except Exception as exc:
            # Unregister & fail
            self._unregister_ids(ids)
            raise TransportError(f"WSS send failed: {exc}") from exc

        # Wait for responses
        if len(ids) == 1 and not isinstance(payload, list):
            return waiter_map[ids[0]].wait(self.config.recv_timeout)

        results = [waiter_map[i].wait(self.config.recv_timeout) for i in ids]
        return results

    def _get_id(self, payload: JSONObj) -> Any:
        if not isinstance(payload, Mapping):
            raise TypeError("Expected JSON object payload")
        if "id" not in payload:
            raise TransportError("JSON-RPC payload missing 'id' (required for WSS correlation)")
        return payload["id"]

    def _ensure_connected(self) -> None:
        if self._closed:
            raise TransportError("WSS transport is closed")

        with self._lock:
            if self._ws is not None:
                return

            # connect
            try:
                ws = websocket.create_connection(
                    self.url,
                    timeout=self.config.connect_timeout,
                    header=self._headers_list(self.config.headers),
                )
                self._ws = ws
            except Exception as exc:
                raise TransportError(f"WSS connect failed: {exc}") from exc

            # start receiver thread
            self._rx_thread = threading.Thread(
                target=self._rx_loop, name="fastweb3-wss-rx", daemon=True
            )
            self._rx_thread.start()

    @staticmethod
    def _headers_list(h: Optional[Mapping[str, str]]) -> list[str] | None:
        if not h:
            return None
        return [f"{k}: {v}" for k, v in h.items()]

    def _rx_loop(self) -> None:
        while not self._closed:
            ws = self._ws
            if ws is None:
                time.sleep(self.config.reconnect_backoff_s)
                continue

            try:
                raw = ws.recv()
                if raw is None:
                    raise TransportError("WSS connection closed")
                if not isinstance(raw, str):
                    # websocket-client may return bytes; decode
                    raw = raw.decode("utf-8")

                msg = json.loads(raw)
            except Exception as exc:
                # Drop connection and fail inflight; next send reconnects
                with self._lock:
                    if self._ws is not None:
                        try:
                            self._ws.close()
                        finally:
                            self._ws = None
                self._fail_all_inflight(TransportError(f"WSS recv failed: {exc}"))
                time.sleep(self.config.reconnect_backoff_s)
                continue

            # Dispatch: can be dict or list[dict]
            try:
                if isinstance(msg, list):
                    for item in msg:
                        self._dispatch_one(item)
                else:
                    self._dispatch_one(msg)
            except Exception as exc:
                # If decoding/dispatch fails, treat as fatal to inflight but keep socket
                self._fail_all_inflight(TransportError(f"WSS dispatch failed: {exc}"))

    def _dispatch_one(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            raise TypeError(f"Expected JSON object response, got {type(msg).__name__}")
        if "id" not in msg:
            # Could be subscription push. Ignore for now.
            return

        rid = msg["id"]
        waiter: Optional[_Waiter] = None
        with self._inflight_lock:
            waiter = self._inflight.pop(rid, None)

        if waiter is None:
            # Unknown id; ignore.
            return

        waiter.set_result(msg)

    def _fail_all_inflight(self, exc: Exception) -> None:
        with self._inflight_lock:
            inflight = list(self._inflight.values())
            self._inflight.clear()
        for w in inflight:
            w.set_exc(exc)

    def _unregister_ids(self, ids: list[Any]) -> None:
        with self._inflight_lock:
            for i in ids:
                self._inflight.pop(i, None)
