# tests/unit/test_provider_hedging.py
from __future__ import annotations

import threading
import time
from typing import Any

from fastweb3.provider import Provider, _EndpointState


class FakeEndpoint:
    """
    Minimal fake for Provider hedging tests.

    Provider uses:
      ep.request_batch(("eth_blockNumber", (), to_int), (method, params, formatter))
        -> returns (tip, result)

      ep.request_batch(("eth_blockNumber", (), to_int), *user_calls)
        -> returns [tip, *user_results]
    """

    def __init__(self, target: str, *, delay_s: float, tip: int, result: Any):
        self.target = target
        self.delay_s = float(delay_s)
        self.tip = tip
        self.result = result
        self.started = threading.Event()
        self.calls = 0

    def request(self, method, params, formatter=None):
        raise AssertionError("request() should not be used in these tests")

    def request_batch(self, *calls):
        self.started.set()
        self.calls += 1
        if self.delay_s:
            time.sleep(self.delay_s)

        # Single-call path: Provider expects a tuple (tip, result)
        if len(calls) == 2 and isinstance(calls[1], tuple):
            return (self.tip, self.result)

        # Batch path: Provider expects list [tip, *user_results]
        user_calls = calls[1:]
        return [self.tip, *([self.result] * len(user_calls))]

    def close(self) -> None:
        return


def _install_fakes(p: Provider, ep1: FakeEndpoint, ep2: FakeEndpoint) -> None:
    """
    Replace Provider's cached endpoints/state with our fakes.
    This is intentionally invasive, but keeps tests fast and deterministic.

    We also disable pool_manager influence by setting desired_pool_size=2 and pool_manager=None.
    """
    p._eps_by_target = {ep1.target: ep1, ep2.target: ep2}  # type: ignore[attr-defined]
    p._state = {ep1: _EndpointState(), ep2: _EndpointState()}  # type: ignore[attr-defined]
    p.pool_manager = None
    p.desired_pool_size = 2


def _st(p: Provider, ep: FakeEndpoint) -> _EndpointState:
    return p._state[ep]  # type: ignore[attr-defined]


def test_hedge_second_wins_returns_second_and_demotes_first() -> None:
    p = Provider(
        internal_endpoints=["http://a", "http://b"],
        hedge_after_seconds=0.05,
        hedge_slow_cooldown_seconds=5.0,
    )

    ep1 = FakeEndpoint("http://a", delay_s=0.20, tip=100, result="A")
    ep2 = FakeEndpoint("http://b", delay_s=0.0, tip=101, result="B")
    _install_fakes(p, ep1, ep2)

    out = p.request("eth_chainId", (), route="pool")
    assert out == "B"

    now = time.time()
    assert _st(p, ep1).slow_cooldown_until > now
    assert _st(p, ep2).slow_cooldown_until <= now


def test_hedge_first_wins_before_delay_does_not_start_second() -> None:
    p = Provider(
        internal_endpoints=["http://a", "http://b"],
        hedge_after_seconds=0.20,
        hedge_slow_cooldown_seconds=5.0,
    )

    ep1 = FakeEndpoint("http://a", delay_s=0.0, tip=100, result="A")
    ep2 = FakeEndpoint("http://b", delay_s=0.0, tip=101, result="B")
    _install_fakes(p, ep1, ep2)

    out = p.request("eth_chainId", (), route="pool")
    assert out == "A"
    assert ep2.started.is_set() is False


def test_loser_does_not_update_best_tip() -> None:
    p = Provider(
        internal_endpoints=["http://a", "http://b"],
        hedge_after_seconds=0.05,
        hedge_slow_cooldown_seconds=5.0,
    )

    # Loser has higher tip but is slower; it must not update best_tip after we return.
    ep1 = FakeEndpoint("http://a", delay_s=0.20, tip=999, result="A")
    ep2 = FakeEndpoint("http://b", delay_s=0.0, tip=1, result="B")
    _install_fakes(p, ep1, ep2)

    out = p.request("eth_chainId", (), route="pool")
    assert out == "B"

    assert p._best_tip == 1  # type: ignore[attr-defined]

    # Wait long enough for loser thread to finish if it's going to.
    time.sleep(0.25)

    # Must remain the winner's tip.
    assert p._best_tip == 1  # type: ignore[attr-defined]


def test_batch_hedge_second_wins_and_demotes_first() -> None:
    p = Provider(
        internal_endpoints=["http://a", "http://b"],
        hedge_after_seconds=0.05,
        hedge_slow_cooldown_seconds=5.0,
    )

    ep1 = FakeEndpoint("http://a", delay_s=0.20, tip=100, result="A")
    ep2 = FakeEndpoint("http://b", delay_s=0.0, tip=101, result="B")
    _install_fakes(p, ep1, ep2)

    out = p.request_batch(
        [
            ("eth_chainId", ()),
            ("net_version", ()),
        ],
        route="pool",
    )
    assert out == ["B", "B"]

    now = time.time()
    assert _st(p, ep1).slow_cooldown_until > now
    assert _st(p, ep2).slow_cooldown_until <= now
