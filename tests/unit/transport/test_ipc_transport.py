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

from fastweb3.errors import TransportError
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


def _ipc_server_n(
    sock_path: Path, handlers: list[Callable[[Any], Any]], ready: threading.Event
) -> None:
    """
    Like _ipc_server_once, but can accept multiple sequential connections.

    Each handler is called with the decoded JSON request, and may return:
      - dict/list: encoded as JSON + newline
      - bytes: sent as-is (newline appended if missing)
      - any JSON-serializable scalar: encoded as JSON + newline
      - None: send nothing (close)
    """
    if sock_path.exists():
        sock_path.unlink()

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        srv.bind(str(sock_path))
        # Generous backlog so the client's single-retry logic can't hit ECONNREFUSED
        # if the server is busy handling the previous connection.
        srv.listen(16)
        ready.set()

        for handler in handlers:
            conn, _ = srv.accept()
            with conn:
                data = b""
                while b"\n" not in data:
                    chunk = conn.recv(65536)
                    if not chunk:
                        data = b""
                        break
                    data += chunk
                if not data or b"\n" not in data:
                    continue

                line, _rest = data.split(b"\n", 1)
                req = json.loads(line.decode("utf-8"))

                resp = handler(req)
                if resp is None:
                    continue

                if isinstance(resp, bytes):
                    out = resp
                else:
                    out = json.dumps(resp).encode("utf-8")

                if not out.endswith(b"\n"):
                    out += b"\n"
                conn.sendall(out)
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


def test_ipc_transport_partial_reads_byte_by_byte(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path
    ready = threading.Event()

    def handler(req: Any) -> Any:
        assert isinstance(req, dict)
        return {"jsonrpc": "2.0", "id": req["id"], "result": "ok"}

    th = threading.Thread(target=_ipc_server_once, args=(sock_path, handler, ready), daemon=True)
    th.start()
    assert ready.wait(timeout=1.0)

    # Force _recv_one_json to parse incrementally.
    tr = IPCTransport(
        str(sock_path),
        config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=2.0, read_chunk_bytes=1),
    )
    try:
        resp = tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
        assert resp["result"] == "ok"
    finally:
        tr.close()


def test_ipc_transport_retries_once_on_connection_close_and_succeeds(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path
    ready = threading.Event()

    def close_without_response(_req: Any) -> None:
        return None

    def ok(req: Any) -> Any:
        return {"jsonrpc": "2.0", "id": req["id"], "result": "second"}

    th = threading.Thread(
        target=_ipc_server_n,
        args=(sock_path, [close_without_response, ok], ready),
        daemon=True,
    )
    th.start()
    assert ready.wait(timeout=1.0)

    tr = IPCTransport(
        str(sock_path),
        config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=1.0),
    )
    try:
        # The first attempt gets a connection-close mid-read; IPCTransport should retry once.
        resp = tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
        assert resp["result"] == "second"
    finally:
        tr.close()


def test_ipc_transport_times_out_and_raises_transport_error(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path
    ready = threading.Event()

    def slow(_req: Any) -> None:
        # Keep the connection open but don't respond until after the client's recv timeout.
        time.sleep(0.2)
        return None

    th = threading.Thread(
        target=_ipc_server_n,
        args=(sock_path, [slow, slow], ready),
        daemon=True,
    )
    th.start()
    assert ready.wait(timeout=1.0)

    tr = IPCTransport(
        str(sock_path),
        config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=0.1),
    )
    try:
        with pytest.raises(TransportError, match=r"IPC recv timeout"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
    finally:
        tr.close()


def test_ipc_transport_rejects_non_object_or_array_response(ipc_sock_path: Path) -> None:
    sock_path = ipc_sock_path
    ready = threading.Event()

    def scalar(_req: Any) -> int:
        return 123

    th = threading.Thread(
        target=_ipc_server_n, args=(sock_path, [scalar, scalar], ready), daemon=True
    )
    th.start()
    assert ready.wait(timeout=1.0)

    tr = IPCTransport(
        str(sock_path),
        config=IPCTransportConfig(connect_timeout=1.0, recv_timeout=1.0),
    )
    try:
        with pytest.raises(TransportError, match=r"Expected JSON object or array"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
    finally:
        tr.close()
