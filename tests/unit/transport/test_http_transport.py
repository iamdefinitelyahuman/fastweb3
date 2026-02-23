from __future__ import annotations

import json

import httpx
import pytest

from fastweb3.errors import TransportError
from fastweb3.transport.http import HTTPTransport, HTTPTransportConfig


def _mock_client(handler) -> httpx.Client:
    """
    Create an httpx.Client backed by an in-memory MockTransport.
    """
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_http_transport_send_success_dict() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "https://example.invalid/rpc"
        body = json.loads(request.content.decode("utf-8"))
        assert body["method"] == "eth_chainId"
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": body["id"], "result": "0x1"},
        )

    client = _mock_client(handler)
    t = HTTPTransport("https://example.invalid/rpc", client=client)

    out = t.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
    assert out == {"jsonrpc": "2.0", "id": 1, "result": "0x1"}


def test_http_transport_send_success_list() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        assert isinstance(body, list)
        return httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": body[0]["id"], "result": "0x1"},
                {"jsonrpc": "2.0", "id": body[1]["id"], "result": "0x2"},
            ],
        )

    client = _mock_client(handler)
    t = HTTPTransport("https://example.invalid/rpc", client=client)

    out = t.send(
        [
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
            {"jsonrpc": "2.0", "id": 2, "method": "net_version", "params": []},
        ]
    )
    assert out == [
        {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
        {"jsonrpc": "2.0", "id": 2, "result": "0x2"},
    ]


def test_http_transport_raises_transport_error_on_http_status_error_includes_status_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": "rate limited"})

    client = _mock_client(handler)
    t = HTTPTransport("https://example.invalid/rpc", client=client)

    with pytest.raises(TransportError) as excinfo:
        t.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})

    err = excinfo.value
    # TransportError should preserve status_code for cooldown logic
    assert getattr(err, "status_code", None) == 429


def test_http_transport_raises_type_error_if_response_json_is_not_dict_or_list() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json="nope")  # JSON string

    client = _mock_client(handler)
    t = HTTPTransport("https://example.invalid/rpc", client=client)

    with pytest.raises(TypeError, match=r"Expected JSON object or array"):
        t.send({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})


def test_http_transport_raises_type_error_if_response_json_list_has_non_dict() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"ok": True}, "bad"])

    client = _mock_client(handler)
    t = HTTPTransport("https://example.invalid/rpc", client=client)

    with pytest.raises(TypeError, match=r"Expected JSON array of objects"):
        t.send([{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}])


def test_http_transport_applies_config_headers_when_owning_client() -> None:
    # No network request needed: just verify the client was created with headers.
    cfg = HTTPTransportConfig(headers={"x-foo": "bar"})
    t = HTTPTransport("https://example.invalid/rpc", config=cfg)
    try:
        assert t._owns_client is True  # type: ignore[attr-defined]
        # httpx normalizes header keys; use case-insensitive access
        assert t._client.headers.get("x-foo") == "bar"  # type: ignore[attr-defined]
    finally:
        t.close()
