from __future__ import annotations

import json

import httpx
import pytest

from fw3.errors import RPCError
from fw3.provider import Provider


def _rpc_result(req_id: int, result):
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _rpc_error(req_id: int, code: int, message: str):
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": code, "message": message},
    }


def _response_for(request: httpx.Request, payload) -> httpx.Response:
    return httpx.Response(
        200,
        request=request,
        json=payload,
        headers={"content-type": "application/json"},
    )


class SendDispatcher:
    """Dispatcher used for the partial-retry + tip pinning integration test."""

    def __init__(self):
        self.calls: list[dict] = []

    def __call__(self, client, request: httpx.Request, *args, **kwargs) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        self.calls.append({"url": str(request.url), "body": body})

        methods = [item["method"] for item in body]

        if str(request.url) == "http://a":
            assert methods == ["eth_blockNumber", "eth_call", "eth_call"]
            return _response_for(
                request,
                [
                    _rpc_result(body[0]["id"], "0x64"),
                    _rpc_result(body[1]["id"], "0x111"),
                    _rpc_error(body[2]["id"], -32603, "internal error"),
                ],
            )

        if str(request.url) == "http://b":
            assert methods == ["eth_blockNumber", "eth_call"]
            assert body[1]["params"][-1] == "0x64"
            return _response_for(
                request,
                [
                    _rpc_result(body[0]["id"], "0x65"),
                    _rpc_result(body[1]["id"], "0x222"),
                ],
            )

        raise AssertionError(f"unexpected request: {request.url} {methods}")


def test_request_batch_pool_partial_rpc_error_retries_subset_and_pins_tip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatcher = SendDispatcher()

    def fake_send(self, request: httpx.Request, *args, **kwargs) -> httpx.Response:
        return dispatcher(self, request, *args, **kwargs)

    monkeypatch.setattr(httpx.Client, "send", fake_send)

    p = Provider(
        internal_endpoints=["http://a", "http://b"],
        desired_pool_size=2,
        hedge_after_seconds=None,
    )

    out = p.request_batch(
        [
            ("eth_call", [{"to": "0x1"}, "latest"]),
            ("eth_call", [{"to": "0x2"}, "latest"]),
        ],
        route="pool",
    )

    assert out == ["0x111", "0x222"]
    assert [call["url"] for call in dispatcher.calls] == ["http://a", "http://b"]


def test_request_batch_primary_route_does_not_retry_item_level_rpc_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def fake_send(client, request: httpx.Request, *args, **kwargs) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        calls.append({"url": str(request.url), "body": body})

        return _response_for(
            request,
            [
                _rpc_result(body[0]["id"], "0x64"),
                _rpc_error(body[1]["id"], -32603, "internal error"),
            ],
        )

    monkeypatch.setattr(httpx.Client, "send", fake_send)

    p = Provider(
        internal_endpoints=["http://a", "http://b"],
        desired_pool_size=2,
        hedge_after_seconds=None,
    )
    p.set_primary("http://a")

    out = p.request_batch(
        [
            ("eth_call", [{"to": "0x1"}, "latest"]),
        ],
        route="primary",
    )

    assert len(calls) == 1
    assert calls[0]["url"] == "http://a"
    assert len(out) == 1
    assert isinstance(out[0], RPCError)


def test_request_batch_pool_stops_after_consensus_on_identical_ambiguous_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_urls: list[str] = []

    def fake_send(client, request: httpx.Request, *args, **kwargs) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        seen_urls.append(str(request.url))

        return _response_for(
            request,
            [
                _rpc_result(body[0]["id"], "0x64"),
                _rpc_error(body[1]["id"], -32603, "internal error"),
            ],
        )

    monkeypatch.setattr(httpx.Client, "send", fake_send)

    p = Provider(
        internal_endpoints=["http://a", "http://b", "http://c", "http://d"],
        desired_pool_size=4,
        hedge_after_seconds=None,
    )

    out = p.request_batch(
        [
            ("eth_call", [{"to": "0x1"}, "latest"]),
        ],
        route="pool",
    )

    assert seen_urls == ["http://a", "http://b", "http://c"]
    assert len(out) == 1
    assert isinstance(out[0], RPCError)


def test_request_batch_pool_mixed_partial_resolution_and_remaining_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_send(client, request: httpx.Request, *args, **kwargs) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        methods = [item["method"] for item in body]
        url = str(request.url)

        if url == "http://a":
            return _response_for(
                request,
                [
                    _rpc_result(body[0]["id"], "0x64"),
                    _rpc_result(body[1]["id"], "0x111"),
                    _rpc_error(body[2]["id"], -32603, "internal error"),
                    _rpc_error(body[3]["id"], -32603, "internal error"),
                ],
            )

        if url == "http://b":
            assert methods == ["eth_blockNumber", "eth_call", "eth_call"]
            return _response_for(
                request,
                [
                    _rpc_result(body[0]["id"], "0x65"),
                    _rpc_result(body[1]["id"], "0x222"),
                    _rpc_error(body[2]["id"], -32603, "internal error"),
                ],
            )

        if url == "http://c":
            assert methods == ["eth_blockNumber", "eth_call"]
            return _response_for(
                request,
                [
                    _rpc_result(body[0]["id"], "0x66"),
                    _rpc_error(body[1]["id"], -32603, "internal error"),
                ],
            )

        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(httpx.Client, "send", fake_send)

    p = Provider(
        internal_endpoints=["http://a", "http://b", "http://c"],
        desired_pool_size=3,
        hedge_after_seconds=None,
    )

    out = p.request_batch(
        [
            ("eth_call", [{"to": "0x1"}, "latest"]),
            ("eth_call", [{"to": "0x2"}, "latest"]),
            ("eth_call", [{"to": "0x3"}, "latest"]),
        ],
        route="pool",
    )

    assert out[0] == "0x111"
    assert out[1] == "0x222"
    assert isinstance(out[2], RPCError)
