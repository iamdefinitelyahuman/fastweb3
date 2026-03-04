# tests/transport/test_ws_transport.py
from __future__ import annotations

import json
import threading
from typing import Any, Callable, Optional

import pytest

from fastweb3.errors import TransportError
from fastweb3.transport.ws import WSSTransport, WSSTransportConfig


class _FakeWebSocket:
    """
    Fake websocket-client WebSocket object.
    - send() records messages
    - recv() pops messages from an injected queue (strings)
    - close() marks closed
    """

    def __init__(self, on_send: Optional[Callable[[str], None]] = None) -> None:
        self.sent: list[str] = []
        self._recv_q: list[object] = []
        self.closed = False
        self._cv = threading.Condition()
        self._on_send = on_send

    def queue_recv(self, msg: dict[str, Any] | list[dict[str, Any]] | str | bytes | None) -> None:
        if isinstance(msg, (dict, list)):
            msg = json.dumps(msg, separators=(",", ":"))
        with self._cv:
            self._recv_q.append(msg)
            self._cv.notify_all()

    def send(self, msg: str) -> None:
        if self.closed:
            raise RuntimeError("socket closed")
        if self._on_send is not None:
            self._on_send(msg)
        self.sent.append(msg)

    def recv(self) -> Any:
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

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

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

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

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

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

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


def test_wss_transport_times_out_waiting_for_response(monkeypatch: pytest.MonkeyPatch) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=0.1),
    )
    try:
        with pytest.raises(TransportError, match=r"WSS recv timeout"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
    finally:
        tr.close()


def test_wss_transport_decodes_bytes_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0),
    )
    try:
        msg = json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0x1"}).encode("utf-8")
        fake.queue_recv(msg)

        resp = tr.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
        assert resp["result"] == "0x1"
    finally:
        tr.close()


def test_wss_transport_batch_response_in_single_list_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0),
    )
    try:
        # A server may respond with a single JSON array message.
        fake.queue_recv(
            [
                {"jsonrpc": "2.0", "id": 2, "result": "B"},
                {"jsonrpc": "2.0", "id": 1, "result": "A"},
            ]
        )

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


def test_wss_transport_duplicate_inflight_request_id_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    send_started = threading.Event()
    fake = _FakeWebSocket(on_send=lambda _msg: send_started.set())

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=10.0),
    )
    try:
        out: dict[str, Any] = {}

        def _worker() -> None:
            try:
                tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
            except Exception as exc:  # pragma: no cover
                out["exc"] = exc

        th = threading.Thread(target=_worker)
        th.start()

        assert send_started.wait(timeout=1.0)

        with pytest.raises(TransportError, match=r"Duplicate in-flight request id"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
    finally:
        tr.close()

    th.join(timeout=1.0)
    assert not th.is_alive()

    # The first request should be unblocked by close().
    assert isinstance(out.get("exc"), TransportError)
    assert "WSS transport closed" in str(out["exc"])


def test_wss_transport_reconnects_after_recv_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    # First connection will deliver invalid JSON *after* we send the request, to avoid
    # racing the send() path (which asserts _ws is not None).
    fake1 = _FakeWebSocket()
    fake2 = _FakeWebSocket()

    def on_send(_msg: str) -> None:
        fake1.queue_recv("not-json")

    fake1._on_send = on_send

    # Second connection will respond normally.
    fake2.queue_recv({"jsonrpc": "2.0", "id": 2, "result": "ok"})

    calls = {"n": 0}
    fakes = [fake1, fake2]

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        calls["n"] += 1
        return fakes[calls["n"] - 1]

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0, reconnect_backoff_s=0.0),
    )
    try:
        with pytest.raises(TransportError, match=r"WSS recv failed"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})

        resp2 = tr.send({"jsonrpc": "2.0", "id": 2, "method": "m", "params": []})
        assert resp2["result"] == "ok"
        assert calls["n"] == 2
    finally:
        tr.close()


def test_wss_transport_connect_failure_surfaces_as_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    def fake_create_connection(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("no connect")

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.1, recv_timeout=0.1),
    )
    try:
        with pytest.raises(TransportError, match=r"WSS connect failed"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
    finally:
        tr.close()


def test_wss_transport_dispatch_failure_fails_inflight(monkeypatch: pytest.MonkeyPatch) -> None:
    import fastweb3.transport.ws as ws_mod

    if ws_mod.websocket is None:
        pytest.skip("websocket-client not installed; skipping WSS transport tests")

    fake = _FakeWebSocket()

    def fake_create_connection(*args: Any, **kwargs: Any) -> _FakeWebSocket:
        return fake

    monkeypatch.setattr(ws_mod.websocket, "create_connection", fake_create_connection)

    tr = WSSTransport(
        "wss://example.invalid",
        config=WSSTransportConfig(connect_timeout=0.5, recv_timeout=1.0, reconnect_backoff_s=0.0),
    )
    try:
        # JSON loads fine but dispatch rejects non-object messages.
        fake.queue_recv(json.dumps("oops"))

        with pytest.raises(TransportError, match=r"WSS dispatch failed"):
            tr.send({"jsonrpc": "2.0", "id": 1, "method": "m", "params": []})
    finally:
        tr.close()
