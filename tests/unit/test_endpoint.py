from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, List, Mapping

import pytest

from fastweb3.endpoint import Endpoint
from fastweb3.errors import RPCError, RPCMalformedResponse, TransportError


@dataclass
class Call:
    payload: Any


class ScriptedTransport:
    """
    Minimal Transport test double:
      - records payloads passed to send()
      - returns queued responses or raises queued exceptions
    """

    def __init__(self) -> None:
        self.calls: List[Call] = []
        self._outcomes: Deque[Any] = deque()

    def queue_return(self, value: Any) -> None:
        self._outcomes.append(("return", value))

    def queue_raise(self, exc: Exception) -> None:
        self._outcomes.append(("raise", exc))

    def send(self, payload: Mapping[str, Any] | List[Mapping[str, Any]]) -> Any:
        self.calls.append(Call(payload=payload))
        if not self._outcomes:
            raise AssertionError("No queued outcomes in ScriptedTransport")
        kind, val = self._outcomes.popleft()
        if kind == "raise":
            raise val
        return val

    def close(self) -> None:
        pass


# -------------------------
# Single request() tests
# -------------------------


def test_request_builds_payload_and_returns_result() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": "0x1"})

    e = Endpoint("https://example.invalid", transport=t)
    out = e.request("eth_chainId", ())

    assert out == "0x1"
    assert len(t.calls) == 1

    payload = t.calls[0].payload
    assert payload["jsonrpc"] == "2.0"
    assert payload["id"] == 1
    assert payload["method"] == "eth_chainId"
    assert payload["params"] == []


def test_request_converts_tuple_params_to_list() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": True})

    e = Endpoint("https://example.invalid", transport=t)
    out = e.request("eth_getBalance", ("0xabc", "latest"))

    assert out is True
    payload = t.calls[0].payload
    assert payload["params"] == ["0xabc", "latest"]


def test_request_applies_formatter() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": "0x2a"})

    e = Endpoint("https://example.invalid", transport=t)
    out = e.request("eth_chainId", (), formatter=lambda x: int(x, 16))

    assert out == 42


def test_request_propagates_transport_error() -> None:
    t = ScriptedTransport()
    t.queue_raise(TransportError("boom"))

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(TransportError, match="boom"):
        e.request("eth_chainId", ())


def test_request_raises_malformed_on_missing_or_invalid_jsonrpc() -> None:
    t = ScriptedTransport()
    t.queue_return({"id": 1, "result": "ok"})  # missing jsonrpc

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="jsonrpc"):
        e.request("eth_chainId", ())

    t2 = ScriptedTransport()
    t2.queue_return({"jsonrpc": "1.0", "id": 1, "result": "ok"})  # invalid jsonrpc
    e2 = Endpoint("https://example.invalid", transport=t2)
    with pytest.raises(RPCMalformedResponse, match="jsonrpc"):
        e2.request("eth_chainId", ())


def test_request_raises_malformed_on_missing_id() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "result": "ok"})  # missing id

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="Missing id"):
        e.request("eth_chainId", ())


def test_request_raises_rpcerror_from_error_dict_details() -> None:
    t = ScriptedTransport()
    t.queue_return(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32000, "message": "execution reverted", "data": "0xdead"},
        }
    )

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCError) as excinfo:
        e.request("eth_call", ("0xabc", "latest"))

    details = excinfo.value.details
    assert details.code == -32000
    assert details.message == "execution reverted"
    assert details.data == "0xdead"


def test_request_raises_rpcerror_from_non_dict_error() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "error": "lol nope"})

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCError) as excinfo:
        e.request("eth_chainId", ())

    details = excinfo.value.details
    assert details.code is None
    assert details.message == "lol nope"
    assert details.data is None


def test_request_raises_malformed_on_missing_result_when_no_error() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1})  # missing result

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="Missing result"):
        e.request("eth_chainId", ())


def test_request_id_increments_across_calls() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": "a"})
    t.queue_return({"jsonrpc": "2.0", "id": 2, "result": "b"})

    e = Endpoint("https://example.invalid", transport=t)
    assert e.request("m1", ()) == "a"
    assert e.request("m2", ()) == "b"

    assert t.calls[0].payload["id"] == 1
    assert t.calls[1].payload["id"] == 2


def test_request_raises_malformed_if_transport_returns_non_dict() -> None:
    t = ScriptedTransport()
    t.queue_return([{"jsonrpc": "2.0", "id": 1, "result": "nope"}])  # list in single-call

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="Single response must be a JSON object"):
        e.request("eth_chainId", ())


# -------------------------
# request_batch() tests
# -------------------------


def test_request_batch_builds_payload_and_reorders_by_id() -> None:
    t = ScriptedTransport()

    # Endpoint will allocate ids starting at 1 then 2.
    # Return the responses out of order to ensure reordering works.
    t.queue_return(
        [
            {"jsonrpc": "2.0", "id": 2, "result": "second"},
            {"jsonrpc": "2.0", "id": 1, "result": "first"},
        ]
    )

    e = Endpoint("https://example.invalid", transport=t)
    out = e.request_batch(("a", ()), ("b", ()))

    assert out == ["first", "second"]

    payload = t.calls[0].payload
    assert isinstance(payload, list)
    assert payload[0]["id"] == 1
    assert payload[1]["id"] == 2
    assert payload[0]["params"] == []
    assert payload[1]["params"] == []


def test_request_batch_requires_list_response() -> None:
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": "nope"})  # not a list

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="must be a list"):
        e.request_batch(("a", ()))


def test_request_batch_validates_items_and_ids() -> None:
    # Non-dict item
    t = ScriptedTransport()
    t.queue_return([{"jsonrpc": "2.0", "id": 1, "result": "ok"}, "bad"])
    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="Batch item must be dict"):
        e.request_batch(("a", ()))

    # Missing id
    t2 = ScriptedTransport()
    t2.queue_return([{"jsonrpc": "2.0", "result": "ok"}])
    e2 = Endpoint("https://example.invalid", transport=t2)
    with pytest.raises(RPCMalformedResponse, match="Missing id"):
        e2.request_batch(("a", ()))

    # Non-int id
    t3 = ScriptedTransport()
    t3.queue_return([{"jsonrpc": "2.0", "id": "1", "result": "ok"}])
    e3 = Endpoint("https://example.invalid", transport=t3)
    with pytest.raises(RPCMalformedResponse, match="Non-int id"):
        e3.request_batch(("a", ()))

    # Duplicate id
    t4 = ScriptedTransport()
    t4.queue_return(
        [
            {"jsonrpc": "2.0", "id": 1, "result": "ok"},
            {"jsonrpc": "2.0", "id": 1, "result": "ok2"},
        ]
    )
    e4 = Endpoint("https://example.invalid", transport=t4)
    with pytest.raises(RPCMalformedResponse, match="Duplicate id"):
        e4.request_batch(("a", ()))


def test_request_batch_requires_all_requested_ids_present() -> None:
    t = ScriptedTransport()
    # Request will allocate ids [1,2], but response only includes id 1
    t.queue_return([{"jsonrpc": "2.0", "id": 1, "result": "only"}])

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="Missing id 2"):
        e.request_batch(("a", ()), ("b", ()))


def test_request_batch_returns_rpcerror_on_error_item() -> None:
    t = ScriptedTransport()
    t.queue_return(
        [
            {"jsonrpc": "2.0", "id": 1, "result": "ok"},
            {"jsonrpc": "2.0", "id": 2, "error": {"code": 123, "message": "bad", "data": None}},
        ]
    )

    e = Endpoint("https://example.invalid", transport=t)
    results = e.request_batch(("a", ()), ("b", ()))

    assert len(results) == 2
    assert results[0] == "ok"
    assert isinstance(results[1], RPCError)
    assert results[1].details.code == 123
    assert results[1].details.message == "bad"


def test_request_batch_raises_malformed_on_missing_result_when_no_error() -> None:
    t = ScriptedTransport()
    t.queue_return(
        [
            {"jsonrpc": "2.0", "id": 1, "result": "ok"},
            {"jsonrpc": "2.0", "id": 2},  # missing result
        ]
    )

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(RPCMalformedResponse, match="Missing result"):
        e.request_batch(("a", ()), ("b", ()))


def test_request_batch_applies_formatters_positionally() -> None:
    t = ScriptedTransport()
    t.queue_return(
        [
            {"jsonrpc": "2.0", "id": 1, "result": "0x2a"},
            {"jsonrpc": "2.0", "id": 2, "result": "0x2b"},
        ]
    )

    e = Endpoint("https://example.invalid", transport=t)
    out = e.request_batch(
        ("a", (), None),
        ("b", (), lambda x: int(x, 16)),
    )

    assert out == ["0x2a", 43]


def test_request_batch_propagates_transport_error() -> None:
    t = ScriptedTransport()
    t.queue_raise(TransportError("timeout"))

    e = Endpoint("https://example.invalid", transport=t)
    with pytest.raises(TransportError, match="timeout"):
        e.request_batch(("a", ()))


# -------------------------
# Factory / scheme tests
# -------------------------


def test_endpoint_uses_factory_for_wss_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Endpoint should be transport-agnostic: if no transport is provided,
    it should call make_transport(url) and work for wss:// targets.
    """
    import fastweb3.endpoint as endpoint_mod

    seen: list[str] = []
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": "0x1"})

    def fake_make_transport(url: str, **kwargs: Any) -> ScriptedTransport:
        seen.append(url)
        return t

    monkeypatch.setattr(endpoint_mod, "make_transport", fake_make_transport)

    e = Endpoint("wss://example.invalid")  # no transport passed
    out = e.request("eth_chainId", ())

    assert out == "0x1"
    assert seen == ["wss://example.invalid"]
    assert t.calls[0].payload["method"] == "eth_chainId"


def test_endpoint_uses_factory_for_ipc_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Same as above but for ipc:// targets.
    """
    import fastweb3.endpoint as endpoint_mod

    seen: list[str] = []
    t = ScriptedTransport()
    t.queue_return({"jsonrpc": "2.0", "id": 1, "result": "0x2a"})

    def fake_make_transport(url: str, **kwargs: Any) -> ScriptedTransport:
        seen.append(url)
        return t

    monkeypatch.setattr(endpoint_mod, "make_transport", fake_make_transport)

    e = Endpoint("ipc:///tmp/geth.ipc")  # no transport passed
    out = e.request("eth_chainId", ())

    assert out == "0x2a"
    assert seen == ["ipc:///tmp/geth.ipc"]


def test_endpoint_factory_transport_supports_batch_for_wss(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Endpoint.request_batch() should work regardless of scheme as long as transport supports batch
    """
    import fastweb3.endpoint as endpoint_mod

    t = ScriptedTransport()
    t.queue_return(
        [
            {"jsonrpc": "2.0", "id": 2, "result": "B"},
            {"jsonrpc": "2.0", "id": 1, "result": "A"},
        ]
    )

    def fake_make_transport(url: str, **kwargs: Any) -> ScriptedTransport:
        assert url.startswith("wss://")
        return t

    monkeypatch.setattr(endpoint_mod, "make_transport", fake_make_transport)

    e = Endpoint("wss://example.invalid")
    out = e.request_batch(("a", ()), ("b", ()))

    assert out == ["A", "B"]
    payload = t.calls[0].payload
    assert isinstance(payload, list)
    assert [p["id"] for p in payload] == [1, 2]
