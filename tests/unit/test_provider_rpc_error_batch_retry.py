from __future__ import annotations

import pytest

from fw3.errors import RPCError, RPCErrorDetails
from fw3.provider.provider import Provider
from fw3.provider.types import _BatchCall


class FakeEndpoint:
    def __init__(self, url: str) -> None:
        self.url = url

    def __repr__(self) -> str:
        return f"FakeEndpoint({self.url})"


def make_rpc_error(code: int, message: str):
    return RPCError(RPCErrorDetails(code=code, message=message))


def make_provider() -> Provider:
    return Provider(pool_manager=object(), desired_pool_size=3, hedge_after_seconds=None)


def make_call(method: str, params):
    return _BatchCall(method=method, params=list(params), formatter=None, freshness=None)


def test_batch_retry_primary_route_bypasses_rpc_error_retry_logic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = make_provider()
    initial_ep = FakeEndpoint("ep-a")
    err = make_rpc_error(-32603, "internal error")
    results = [err]
    calls = [make_call("eth_call", [{"to": "0x0"}, "latest"])]

    monkeypatch.setattr(
        provider,
        "_pool_candidates",
        lambda: (_ for _ in ()).throw(AssertionError("should not inspect pool")),
    )
    monkeypatch.setattr(
        provider,
        "_mark_failure",
        lambda ep, exc: (_ for _ in ()).throw(AssertionError("should not demote")),
    )

    out = provider._maybe_retry_rpc_errors_in_batch(
        ep=initial_ep,
        calls=calls,
        results=results,
        returned_tip=100,
        route="primary",
    )
    assert out is results
    assert out[0] is err


def test_batch_retry_retries_only_ambiguous_items_and_merges_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = make_provider()
    ep_a = FakeEndpoint("ep-a")
    ep_b = FakeEndpoint("ep-b")

    deterministic = make_rpc_error(-32000, "execution reverted: nope")
    ambiguous = make_rpc_error(-32603, "internal error")

    calls = [
        make_call("eth_call", [{"to": "0x1"}, "latest"]),
        make_call("eth_call", [{"to": "0x2"}, "latest"]),
    ]
    results = [deterministic, ambiguous]

    seen_calls: list[list[_BatchCall]] = []

    monkeypatch.setattr(provider, "_pool_candidates", lambda: [ep_a, ep_b])
    monkeypatch.setattr(provider, "_eligible_endpoints", lambda eps: list(eps))
    monkeypatch.setattr(provider, "_last_tip", lambda ep: 0)
    monkeypatch.setattr(provider, "_mark_failure", lambda ep, exc: None)

    def fake_attempt_batch(ep, retry_calls):
        seen_calls.append(list(retry_calls))
        assert ep is ep_b
        return (["fixed"], None, 100)

    monkeypatch.setattr(provider, "_attempt_batch", fake_attempt_batch)

    out = provider._maybe_retry_rpc_errors_in_batch(
        ep=ep_a,
        calls=calls,
        results=list(results),
        returned_tip=100,
        route="pool",
    )

    assert len(seen_calls) == 1
    assert len(seen_calls[0]) == 1
    assert seen_calls[0][0].method == "eth_call"
    assert out[0] is deterministic
    assert out[1] == "fixed"


def test_batch_retry_demotes_initial_endpoint_for_node_health_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = make_provider()
    ep_a = FakeEndpoint("ep-a")
    ep_b = FakeEndpoint("ep-b")

    err = make_rpc_error(-32000, "missing trie node 0xabc")
    calls = [make_call("eth_call", [{"to": "0x1"}, "latest"])]
    results = [err]

    demotions: list[str] = []

    monkeypatch.setattr(provider, "_pool_candidates", lambda: [ep_a, ep_b])
    monkeypatch.setattr(provider, "_eligible_endpoints", lambda eps: list(eps))
    monkeypatch.setattr(provider, "_last_tip", lambda ep: 0)
    monkeypatch.setattr(provider, "_mark_failure", lambda ep, exc: demotions.append(ep.url))
    monkeypatch.setattr(provider, "_attempt_batch", lambda ep, retry_calls: (["ok"], None, 100))

    out = provider._maybe_retry_rpc_errors_in_batch(
        ep=ep_a,
        calls=calls,
        results=list(results),
        returned_tip=100,
        route="pool",
    )

    assert out == ["ok"]
    assert demotions == ["ep-a"]


def test_batch_retry_stops_after_three_matching_ambiguous_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = make_provider()
    ep_a = FakeEndpoint("ep-a")
    ep_b = FakeEndpoint("ep-b")
    ep_c = FakeEndpoint("ep-c")

    first = make_rpc_error(-32603, "internal error")
    second = make_rpc_error(-32603, "internal error")
    third = make_rpc_error(-32603, "internal error")

    calls = [make_call("eth_call", [{"to": "0x1"}, "latest"])]
    results = [first]

    monkeypatch.setattr(provider, "_pool_candidates", lambda: [ep_a, ep_b, ep_c])
    monkeypatch.setattr(provider, "_eligible_endpoints", lambda eps: list(eps))
    monkeypatch.setattr(provider, "_last_tip", lambda ep: 0)
    monkeypatch.setattr(provider, "_mark_failure", lambda ep, exc: None)

    attempts = []

    def fake_attempt_batch(ep, retry_calls):
        attempts.append(ep.url)
        return ([second], None, 100) if ep is ep_b else ([third], None, 100)

    monkeypatch.setattr(provider, "_attempt_batch", fake_attempt_batch)

    out = provider._maybe_retry_rpc_errors_in_batch(
        ep=ep_a,
        calls=calls,
        results=list(results),
        returned_tip=100,
        route="pool",
    )

    assert attempts == ["ep-b", "ep-c"]
    assert isinstance(out[0], RPCError)


def test_batch_retry_can_partially_resolve_across_multiple_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = make_provider()
    ep_a = FakeEndpoint("ep-a")
    ep_b = FakeEndpoint("ep-b")
    ep_c = FakeEndpoint("ep-c")

    err1 = make_rpc_error(-32603, "internal error")
    err2 = make_rpc_error(-32603, "internal error")
    err2b = make_rpc_error(-32603, "internal error")
    err2c = make_rpc_error(-32603, "internal error")

    calls = [
        make_call("eth_call", [{"to": "0x1"}, "latest"]),
        make_call("eth_call", [{"to": "0x2"}, "latest"]),
        make_call("eth_call", [{"to": "0x3"}, "latest"]),
    ]
    results = [err1, err2, 33]

    monkeypatch.setattr(provider, "_pool_candidates", lambda: [ep_a, ep_b, ep_c])
    monkeypatch.setattr(provider, "_eligible_endpoints", lambda eps: list(eps))
    monkeypatch.setattr(provider, "_last_tip", lambda ep: 0)
    monkeypatch.setattr(provider, "_mark_failure", lambda ep, exc: None)

    seen_lengths: list[int] = []

    def fake_attempt_batch(ep, retry_calls):
        seen_lengths.append(len(retry_calls))
        if ep is ep_b:
            return (["fixed-one", err2b], None, 100)
        return ([err2c], None, 100)

    monkeypatch.setattr(provider, "_attempt_batch", fake_attempt_batch)

    out = provider._maybe_retry_rpc_errors_in_batch(
        ep=ep_a,
        calls=calls,
        results=list(results),
        returned_tip=100,
        route="pool",
    )

    assert seen_lengths == [2, 1]
    assert out[0] == "fixed-one"
    assert isinstance(out[1], RPCError)
    assert out[2] == 33


def test_batch_retry_pins_retry_subset_to_original_tip(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = make_provider()
    ep_a = FakeEndpoint("ep-a")
    ep_b = FakeEndpoint("ep-b")

    err = make_rpc_error(-32603, "internal error")
    calls = [make_call("eth_call", [{"to": "0x1"}, "latest"])]
    results = [err]

    captured_calls: list[_BatchCall] = []

    monkeypatch.setattr(provider, "_pool_candidates", lambda: [ep_a, ep_b])
    monkeypatch.setattr(provider, "_eligible_endpoints", lambda eps: list(eps))
    monkeypatch.setattr(provider, "_last_tip", lambda ep: 0)
    monkeypatch.setattr(provider, "_mark_failure", lambda ep, exc: None)

    def fake_attempt_batch(ep, retry_calls):
        captured_calls.extend(retry_calls)
        return (["ok"], None, 100)

    monkeypatch.setattr(provider, "_attempt_batch", fake_attempt_batch)

    out = provider._maybe_retry_rpc_errors_in_batch(
        ep=ep_a,
        calls=calls,
        results=list(results),
        returned_tip=255,
        route="pool",
    )

    assert out == ["ok"]
    assert len(captured_calls) == 1
    assert list(captured_calls[0].params) == [{"to": "0x1"}, "0xff"]
