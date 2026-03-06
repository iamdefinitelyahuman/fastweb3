# tests/unit/test_provider_batch.py
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque

import pytest

import fastweb3.provider as provider_mod
import fastweb3.provider.endpoint_selection as es_mod
from fastweb3.errors import AllEndpointsFailed, RPCError, RPCErrorDetails, TransportError
from fastweb3.formatters import to_int


class _StubPoolManager:
    def best_urls(self, needed: int, await_first: bool) -> list[str]:
        # Provider will merge internal endpoints + these.
        return []


@dataclass
class _Outcome:
    value: Any | None = None
    exc: Exception | None = None


class FakeEndpoint:
    """
    Minimal Endpoint test double for Provider.request_batch tests.

    - Has .target
    - .request_batch() pops scripted outcomes
    - records .batch_calls for assertions
    - .close() is a no-op
    """

    def __init__(self, target: str, *args: Any, **kwargs: Any) -> None:
        self.target = target
        self.closed = False
        self.batch_calls: list[list[tuple[Any, ...]]] = []
        self._outcomes: Deque[_Outcome] = deque()

    def script(self, *outcomes: _Outcome) -> None:
        self._outcomes.extend(outcomes)

    def request_batch(self, *calls: tuple[Any, ...]) -> Any:
        self.batch_calls.append(list(calls))
        if not self._outcomes:
            raise AssertionError(f"{self}.request_batch() called but no scripted outcomes left")
        oc = self._outcomes.popleft()
        if oc.exc is not None:
            raise oc.exc
        return oc.value

    def close(self) -> None:
        self.closed = True

    def __repr__(self) -> str:
        return f"FakeEndpoint({self.target})"


def _rpc_err(code: int = -32000, message: str = "boom") -> RPCError:
    return RPCError(RPCErrorDetails(code=code, message=message, data=None))


def test_provider_request_batch_primary_no_tip_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    # No internal endpoints; pool_manager None => desired_pool_size ends up 0
    p = provider_mod.Provider(internal_endpoints=None, pool_manager=None)

    p.set_primary("http://a")
    ep = p._get_or_create_endpoint("http://a")
    assert isinstance(ep, FakeEndpoint)

    ep.script(_Outcome(value=[1, _rpc_err(), "ok"]))

    out = p.request_batch(
        [
            ("eth_getBalance", ("0xabc", "latest"), None, None),
            ("eth_call", ({"to": "0x0"}, "latest"), None, None),
            ("net_version", (), None, None),
        ],
        route="primary",
    )

    assert len(out) == 3
    assert out[0] == 1
    assert isinstance(out[1], RPCError)
    assert out[2] == "ok"

    # No tip probe when desired_pool_size == 0
    assert ep.batch_calls == [
        [
            ("eth_getBalance", ("0xabc", "latest"), None),
            ("eth_call", ({"to": "0x0"}, "latest"), None),
            ("net_version", (), None),
        ]
    ]


def test_provider_request_batch_primary_with_tip_probe_when_internal_endpoints_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    # internal_endpoints forces desired_pool_size >= 1
    p = provider_mod.Provider(internal_endpoints=["http://a"], pool_manager=None)
    p.set_primary("http://a")
    ep = p._get_or_create_endpoint("http://a")
    assert isinstance(ep, FakeEndpoint)

    ep.script(_Outcome(value=[123, "x", "y"]))

    out = p.request_batch([("m1", (), None, None), ("m2", (), None, None)], route="primary")
    assert out == ["x", "y"]

    batch = ep.batch_calls[0]
    assert batch[0][0] == "eth_blockNumber"
    assert batch[0][2] is to_int


def test_provider_request_batch_primary_requires_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)
    p = provider_mod.Provider(internal_endpoints=["http://a"], pool_manager=None)

    with pytest.raises(es_mod.NoPrimaryEndpoint):
        p.request_batch([("net_version", (), None, None)], route="primary")


def test_provider_request_batch_pool_includes_tip_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    p = provider_mod.Provider(
        internal_endpoints=["http://a"],
        pool_manager=_StubPoolManager(),
        desired_pool_size=1,
    )

    ep = p._get_or_create_endpoint("http://a")
    assert isinstance(ep, FakeEndpoint)

    # Tip=123 then two results
    ep.script(_Outcome(value=[123, "x", "y"]))

    out = p.request_batch(
        [
            ("foo", (1,), None, None),
            ("bar", (), None, None),
        ],
        route="pool",
    )

    assert out == ["x", "y"]

    # First call in the batch should be the provider-added tip probe
    batch = ep.batch_calls[0]
    assert batch[0][0] == "eth_blockNumber"
    assert batch[0][2] is to_int
    assert batch[1] == ("foo", (1,), None)
    assert batch[2] == ("bar", (), None)


def test_provider_request_batch_pool_rpc_errors_returned_in_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    p = provider_mod.Provider(
        internal_endpoints=["http://a"],
        pool_manager=_StubPoolManager(),
        desired_pool_size=1,
    )
    ep = p._get_or_create_endpoint("http://a")
    assert isinstance(ep, FakeEndpoint)

    ep.script(_Outcome(value=[100, _rpc_err(message="call failed"), "ok"]))

    out = p.request_batch(
        [
            ("call1", (), None, None),
            ("call2", (), None, None),
        ],
        route="pool",
    )

    assert len(out) == 2
    assert isinstance(out[0], RPCError)
    assert out[1] == "ok"


def test_provider_request_batch_freshness_rejects_entire_batch_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    p = provider_mod.Provider(
        internal_endpoints=["http://a", "http://b"],
        pool_manager=_StubPoolManager(),
        desired_pool_size=2,
        retry_policy_pool=provider_mod.RetryPolicy(max_attempts=2, backoff_seconds=0.0),
    )

    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)

    a.script(_Outcome(value=[100, "a1", "a2"]))
    b.script(_Outcome(value=[250, "b1", "b2"]))

    def need_tip_ge_200(_val: Any, _required_tip: int, returned_tip: int) -> bool:
        return returned_tip >= 200

    out = p.request_batch(
        [
            ("m1", (), None, need_tip_ge_200),
            ("m2", (), None, need_tip_ge_200),
        ],
        route="pool",
    )

    assert out == ["b1", "b2"]
    assert len(a.batch_calls) == 1
    assert len(b.batch_calls) == 1


def test_provider_request_batch_tip_probe_rpcerror_rotates_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    p = provider_mod.Provider(
        internal_endpoints=["http://a", "http://b"],
        pool_manager=_StubPoolManager(),
        desired_pool_size=2,
        retry_policy_pool=provider_mod.RetryPolicy(max_attempts=2, backoff_seconds=0.0),
    )

    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)

    a.script(_Outcome(value=[_rpc_err(message="tip failed"), "x"]))
    b.script(_Outcome(value=[200, "ok"]))

    out = p.request_batch([("m", (), None, None)], route="pool")
    assert out == ["ok"]

    assert len(a.batch_calls) == 1
    assert len(b.batch_calls) == 1


def test_provider_request_batch_transport_errors_raise_allendpointsfailed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)

    p = provider_mod.Provider(
        internal_endpoints=["http://a", "http://b"],
        pool_manager=_StubPoolManager(),
        desired_pool_size=2,
        retry_policy_pool=provider_mod.RetryPolicy(max_attempts=2, backoff_seconds=0.0),
    )

    a = p._get_or_create_endpoint("http://a")
    b = p._get_or_create_endpoint("http://b")
    assert isinstance(a, FakeEndpoint)
    assert isinstance(b, FakeEndpoint)

    a.script(_Outcome(exc=TransportError("down")))
    b.script(_Outcome(exc=TransportError("down too")))

    with pytest.raises(AllEndpointsFailed):
        p.request_batch([("m", (), None, None)], route="pool")

    assert len(a.batch_calls) == 1
    assert len(b.batch_calls) == 1
