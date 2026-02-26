# tests/transport/test_ipc_transport.py
from __future__ import annotations

import json
import socket
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

import pytest

from fastweb3.transport.ipc import IPCTransport, IPCTransportConfig


def _supports_ipc_unix_socket() -> bool:
    # "IPC" here is AF_UNIX domain sockets. Some Windows Python builds don't expose it.
    return hasattr(socket, "AF_UNIX")


@pytest.fixture
def ipc_sock_path() -> Path:
    """
    Use a short path for AF_UNIX sockets.

    On macOS, AF_UNIX path length is small, and pytest's tmp_path in CI can be
    too long (e.g. /private/var/folders/.../pytest-0/...).
    """
    if not _supports_ipc_unix_socket():
        pytest.skip("AF_UNIX (unix domain sockets) not supported on this platform")

    # Prefer a short base dir. On Unix runners, /tmp is available and short.
    base_dir = "/tmp" if Path("/tmp").exists() else None
    d = Path(tempfile.mkdtemp(prefix="fw3_ipc_", dir=base_dir))
    return d / "node.ipc"


def _ipc_server_once(
    sock_path: Path, handler: Callable[[Any], Any], ready: threading.Event
) -> None:
    """
    Minimal UNIX socket JSON-RPC-ish server:
      - binds/listens and signals readiness
      - accepts one connection
      - reads one JSON value (newline-delimited)
      - writes one JSON response (newline-delimited)
      - closes and unlinks
    """
    if sock_path.exists():
        sock_path.unlink()

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        srv.bind(str(sock_path))
        srv.listen(1)
        ready.set()

        conn, _ = srv.accept()
        with conn:
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                data += chunk

            line, _rest = data.split(b"\n", 1)
            req = json.loads(line.decode("utf-8"))

            resp = handler(req)
            conn.sendall(json.dumps(resp).encode("utf-8") + b"\n")
    finally:
        srv.close()
        try:
            sock_path.unlink()
        except FileNotFoundError:
            pass


def test_ipc_transport_single_request_roundtrip(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path
    ready = threading.Event()

    def handler(req: Any) -> Any:
        assert isinstance(req, dict)
        assert req["jsonrpc"] == "2.0"
        assert req["id"] == 1
        assert req["method"] == "eth_chainId"
        return {"jsonrpc": "2.0", "id": req["id"], "result": "0x1"}

    th = threading.Thread(target=_ipc_server_once, args=(sock_path, handler, ready), daemon=True)
    th.start()
    assert ready.wait(timeout=1.0)

    tr = IPCTransport(
        str(sock_path), config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=2.0)
    )
    try:
        resp = tr.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
        assert resp["result"] == "0x1"
    finally:
        tr.close()


def test_ipc_transport_batch_request_roundtrip(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path
    ready = threading.Event()

    def handler(req: Any) -> Any:
        assert isinstance(req, list)
        assert [x["id"] for x in req] == [1, 2]
        return [
            {"jsonrpc": "2.0", "id": 2, "result": "B"},
            {"jsonrpc": "2.0", "id": 1, "result": "A"},
        ]

    th = threading.Thread(target=_ipc_server_once, args=(sock_path, handler, ready), daemon=True)
    th.start()
    assert ready.wait(timeout=1.0)

    tr = IPCTransport(
        str(sock_path), config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=2.0)
    )
    try:
        resp = tr.send(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "a", "params": []},
                {"jsonrpc": "2.0", "id": 2, "method": "b", "params": []},
            ]
        )
        assert isinstance(resp, list)
        assert [x["id"] for x in resp] == [2, 1]
    finally:
        tr.close()


def test_ipc_transport_reconnects_after_server_closes(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path

    tr = IPCTransport(
        str(sock_path), config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=2.0)
    )
    try:
        ready1 = threading.Event()

        def handler1(req: Any) -> Any:
            return {"jsonrpc": "2.0", "id": req["id"], "result": "first"}

        th1 = threading.Thread(
            target=_ipc_server_once, args=(sock_path, handler1, ready1), daemon=True
        )
        th1.start()
        assert ready1.wait(timeout=1.0)

        resp1 = tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
        assert resp1["result"] == "first"

        # Give the server a moment to close/unlink before the next accept/bind.
        time.sleep(0.05)

        ready2 = threading.Event()

        def handler2(req: Any) -> Any:
            return {"jsonrpc": "2.0", "id": req["id"], "result": "second"}

        th2 = threading.Thread(
            target=_ipc_server_once, args=(sock_path, handler2, ready2), daemon=True
        )
        th2.start()
        assert ready2.wait(timeout=1.0)

        resp2 = tr.send({"jsonrpc": "2.0", "id": 2, "method": "m", "params": []})
        assert resp2["result"] == "second"
    finally:
        tr.close()
