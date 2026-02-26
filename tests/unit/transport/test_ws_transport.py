# tests/transport/test_ws_transport.py
from __future__ import annotations

import json
import threading
from typing import Any

import pytest

from fastweb3.transport.ws import WSSTransport, WSSTransportConfig


class _FakeWebSocket:
    """
    Fake websocket-client WebSocket object.
    - send() records messages
    - recv() pops messages from an injected queue (strings)
    - close() marks closed
    """

    def __init__(self) -> None:
        self.sent: list[str] = []
        self._recv_q: list[str] = []
        self.closed = False
        self._cv = threading.Condition()

    def queue_recv(self, msg: dict[str, Any] | list[dict[str, Any]] | str) -> None:
        if not isinstance(msg, str):
            msg = json.dumps(msg, separators=(",", ":"))
        with self._cv:
            self._recv_q.append(msg)
            self._cv.notify_all()

    def send(self, msg: str) -> None:
        if self.closed:
            raise RuntimeError("socket closed")
        self.sent.append(msg)

    def recv(self) -> str:
        with self._cv:
            while not self._recv_q and not self.closed:
                self._cv.wait(timeout=0.5)
            if self.closed:
                raise RuntimeError("socket closed")
            return self._recv_q.pop(0)

    def close(self) -> None:
        with self._cv:
            self.closed = True
            self._cv.notify_all()


def test_wss_transport_single_request_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    import fastweb3.transport.ws as ws_mod

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    # Patch the create_connection used by WSSTransport
    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid", config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0)
    )
    try:
        # Enqueue the response after send is likely registered; order doesn't matter much here.
        fake.queue_recv({"jsonrpc": "2.0", "id": 1, "result": "0x1"})

        resp = tr.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
        assert resp["result"] == "0x1"

        # Ensure it actually sent something
        assert fake.sent, "expected WSSTransport to call send()"
        sent_payload = json.loads(fake.sent[0])
        assert sent_payload["id"] == 1
        assert sent_payload["method"] == "eth_chainId"
    finally:
        tr.close()


def test_wss_transport_batch_request_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    import fastweb3.transport.ws as ws_mod

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid", config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0)
    )
    try:
        # Server may respond as separate messages; our implementation supports that.
        fake.queue_recv({"jsonrpc": "2.0", "id": 2, "result": "B"})
        fake.queue_recv({"jsonrpc": "2.0", "id": 1, "result": "A"})

        resp = tr.send(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "a", "params": []},
                {"jsonrpc": "2.0", "id": 2, "method": "b", "params": []},
            ]
        )
        assert isinstance(resp, list)
        assert [x["id"] for x in resp] == [1, 2]
        assert [x["result"] for x in resp] == ["A", "B"]
    finally:
        tr.close()


def test_wss_transport_ignores_messages_without_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Subscription-like pushes have no 'id'. We ignore them and still satisfy the request.
    """
    import fastweb3.transport.ws as ws_mod

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid", config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0)
    )
    try:
        fake.queue_recv(
            {"jsonrpc": "2.0", "method": "eth_subscription", "params": {"result": "push"}}
        )
        fake.queue_recv({"jsonrpc": "2.0", "id": 9, "result": "ok"})

        resp = tr.send({"jsonrpc": "2.0", "id": 9, "method": "eth_chainId", "params": []})
        assert resp["result"] == "ok"
    finally:
        tr.close()
