"""Transport factory for endpoint targets."""

from __future__ import annotations

import os

from .base import Transport
from .http import HTTPTransport, HTTPTransportConfig
from .ipc import IPCTransport, IPCTransportConfig
from .ws import WSSTransport, WSSTransportConfig


def make_transport(
    target: str,
    *,
    http: HTTPTransportConfig | None = None,
    wss: WSSTransportConfig | None = None,
    ipc: IPCTransportConfig | None = None,
) -> Transport:
    """Create a transport instance for an endpoint target.

    Args:
        target: Endpoint target string. Supported formats:

            * ``http://...`` / ``https://...``
            * ``ws://...`` / ``wss://...`` (requires ``websocket-client``)
            * ``ipc://<path>``
            * absolute file path (treated as IPC)

        http: Optional HTTP transport configuration.
        wss: Optional WebSocket transport configuration.
        ipc: Optional IPC transport configuration.

    Returns:
        A transport instance.

    Raises:
        ValueError: If the target cannot be mapped to a known transport.
    """
    t = target.strip()

    if t.startswith(("http://", "https://")):
        return HTTPTransport(t, config=http)
    if t.startswith(("ws://", "wss://")):
        return WSSTransport(t, config=wss)
    if t.startswith("ipc://"):
        return IPCTransport(t[len("ipc://") :], config=ipc)

    # nice UX: treat absolute paths as IPC
    if os.path.isabs(t):
        return IPCTransport(t, config=ipc)

    raise ValueError(f"Unknown transport target: {target!r}")
