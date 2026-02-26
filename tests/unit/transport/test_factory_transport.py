# tests/transport/test_factory_transport.py
from __future__ import annotations

import os
from pathlib import Path

import pytest

from fastweb3.transport.factory import make_transport
from fastweb3.transport.http import HTTPTransport
from fastweb3.transport.ipc import IPCTransport


def test_factory_creates_http_transport() -> None:
    tr = make_transport("https://example.invalid")
    assert isinstance(tr, HTTPTransport)
    tr.close()


def test_factory_creates_ws_transport_if_available() -> None:
    # WSSTransport depends on websocket-client; if missing, make_transport should raise.
    try:
        from fastweb3.transport.ws import WSSTransport  # noqa: F401
    except Exception:
        pytest.skip("websocket-client not installed; skipping WSS factory test")

    tr = make_transport("wss://example.invalid")
    # Import inside to avoid hard failure if dependency missing
    from fastweb3.transport.ws import WSSTransport

    assert isinstance(tr, WSSTransport)
    tr.close()


def test_factory_creates_ipc_transport_from_ipc_scheme(tmp_path: Path) -> None:
    tr = make_transport(f"ipc://{tmp_path / 'node.ipc'}")
    assert isinstance(tr, IPCTransport)
    tr.close()


def test_factory_creates_ipc_transport_from_absolute_path(tmp_path: Path) -> None:
    p = tmp_path / "geth.ipc"
    # The file doesn't need to exist; factory decision is purely syntactic.
    assert os.path.isabs(str(p))
    tr = make_transport(str(p))
    assert isinstance(tr, IPCTransport)
    tr.close()


def test_factory_rejects_unknown_target() -> None:
    with pytest.raises(ValueError):
        make_transport("ftp://example.invalid")
