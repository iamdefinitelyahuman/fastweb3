# src/fastweb3/transport/ipc.py
"""IPC (Unix domain socket) transport implementation."""

from __future__ import annotations

import json
import socket
import threading
from dataclasses import dataclass
from typing import Optional

from ..errors import TransportError
from .base import JSONPayload, JSONResp


@dataclass(frozen=True)
class IPCTransportConfig:
    """Configuration for `IPCTransport`.

    Attributes:
        connect_timeout: Timeout for connecting to the socket.
        recv_timeout: Timeout for receiving a response.
        send_timeout: Timeout for sending a request.
        read_chunk_bytes: Size of each recv() chunk.
        max_response_bytes: Hard cap on response size to prevent unbounded
            memory use.
    """

    connect_timeout: float = 2.0
    recv_timeout: float = 20.0
    send_timeout: float = 20.0

    # How big the read buffer chunk is
    read_chunk_bytes: int = 64 * 1024

    # Hard cap to avoid unbounded memory if peer misbehaves
    max_response_bytes: int = 10 * 1024 * 1024  # 10MB


class IPCTransport:
    """Synchronous IPC (Unix domain socket) transport for JSON-RPC.

    Notes:
        * Single in-flight request per transport (guarded by a lock).
        * If the socket drops, the next `send()` reconnects.
        * Uses ``socket.settimeout`` for portability.
    """

    def __init__(self, path: str, *, config: IPCTransportConfig | None = None) -> None:
        """Create an IPC transport.

        Args:
            path: Filesystem path to the Unix domain socket.
            config: Optional transport configuration.
        """
        self.path = path
        self.config = config or IPCTransportConfig()

        self._lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._closed = False

    def close(self) -> None:
        """Close the transport and any open socket."""
        self._closed = True
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                finally:
                    self._sock = None

    def _connect(self) -> socket.socket:
        if self._closed:
            raise TransportError("IPC transport is closed")

        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            # connect timeout
            s.settimeout(self.config.connect_timeout)
            s.connect(self.path)

            # After connect: default to blocking; we set per-op timeouts in send()
            s.settimeout(None)
            return s
        except Exception as exc:
            try:
                s.close()
            finally:
                pass
            raise TransportError(f"IPC connect failed: {exc}") from exc

    def _ensure_socket(self) -> socket.socket:
        if self._sock is not None:
            return self._sock
        self._sock = self._connect()
        return self._sock

    def send(self, payload: JSONPayload) -> JSONResp:
        """Send a JSON-RPC payload over IPC.

        Args:
            payload: JSON-RPC payload (single object or batch list).

        Returns:
            Decoded JSON response.

        Raises:
            TransportError: For connection and protocol errors.
            TypeError: If the decoded JSON is not an object or list of objects.
        """
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"

        with self._lock:
            sock = self._ensure_socket()
            try:
                # send with send-timeout
                sock.settimeout(self.config.send_timeout)
                sock.sendall(data)

                # recv with recv-timeout
                sock.settimeout(self.config.recv_timeout)
                return self._recv_one_json(sock)
            except Exception:
                # Drop socket and retry once (fresh reconnect)
                try:
                    sock.close()
                finally:
                    self._sock = None

                sock2 = self._ensure_socket()
                try:
                    sock2.settimeout(self.config.send_timeout)
                    sock2.sendall(data)

                    sock2.settimeout(self.config.recv_timeout)
                    return self._recv_one_json(sock2)
                except Exception as exc2:
                    try:
                        sock2.close()
                    finally:
                        self._sock = None
                    raise TransportError(f"IPC send failed: {exc2}") from exc2
            finally:
                # Leave socket in blocking mode between calls
                try:
                    sock.settimeout(None)
                except Exception:
                    pass

    def _recv_one_json(self, sock: socket.socket) -> JSONResp:
        buf = bytearray()
        decoder = json.JSONDecoder()

        while True:
            if len(buf) > self.config.max_response_bytes:
                raise TransportError("IPC response exceeded max_response_bytes")

            try:
                chunk = sock.recv(self.config.read_chunk_bytes)
            except socket.timeout as exc:
                raise TransportError("IPC recv timeout") from exc
            except Exception as exc:
                raise TransportError(f"IPC recv failed: {exc}") from exc

            if not chunk:
                raise TransportError("IPC connection closed while reading response")

            buf.extend(chunk)

            # Try parse from start; allow leading whitespace/newlines
            try:
                s = buf.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise TransportError(f"IPC response was not valid UTF-8: {exc}") from exc

            s2 = s.lstrip()
            try:
                obj, _end = decoder.raw_decode(s2)
            except json.JSONDecodeError:
                continue  # need more bytes

            # We parsed a full JSON value. Validate shape.
            if not isinstance(obj, (dict, list)):
                raise TypeError(f"Expected JSON object or array, got {type(obj).__name__}")
            if isinstance(obj, list) and not all(isinstance(x, dict) for x in obj):
                raise TypeError("Expected JSON array of objects")
            return obj
