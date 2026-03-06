# tests/unit/test_provider_middlewares.py
from __future__ import annotations

import pytest

import fastweb3.middleware as mw_mod
import fastweb3.provider.endpoint_selection as es_mod
from fastweb3.errors import TransportError
from fastweb3.provider import Provider
from fastweb3.provider.types import _BatchCall


@pytest.fixture(autouse=True)
def _clear_default_registry() -> None:
    mw_mod.clear_default_middlewares()
    yield
    mw_mod.clear_default_middlewares()


class FakeEndpoint:
    def __init__(self, target: str) -> None:
        self.target = target
        self._queue: list[object] = []
        self.calls: list[tuple[str, object]] = []  # ("request"/"batch", ...)

    def queue_return(self, value: object) -> None:
        self._queue.append(value)

    def queue_raise(self, exc: Exception) -> None:
        self._queue.append(exc)

    def request(self, method, params, formatter=None):
        self.calls.append(("request", (method, params, formatter)))
        if not self._queue:
            raise AssertionError("FakeEndpoint has no queued result")
        item = self._queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def request_batch(self, *calls):
        self.calls.append(("batch", calls))
        if not self._queue:
            raise AssertionError("FakeEndpoint has no queued result")
        item = self._queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self) -> None:
        return


@pytest.fixture
def _patch_endpoint(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(es_mod, "Endpoint", FakeEndpoint)
    yield


class RecorderMW:
    def __init__(self, events: list[str], name: str) -> None:
        self._events = events
        self._name = name

    def before_request(self, ctx, calls):
        self._events.append(f"{self._name}:before")
        return calls

    def after_request(self, ctx, calls, results):
        self._events.append(f"{self._name}:after")
        return results

    def on_exception(self, ctx, calls, exc):
        self._events.append(f"{self._name}:exc")
        return exc


def test_middleware_ordering_onion_before_after(_patch_endpoint) -> None:
    # pool_manager=None => desired_pool_size = 0 => Endpoint.request used (single object payload).
    p = Provider([])
    p.set_primary("p1")

    events: list[str] = []
    p.add_middleware(RecorderMW(events, "A"))
    p.add_middleware(RecorderMW(events, "B"))
    p.add_middleware(RecorderMW(events, "C"))

    ep = p._get_or_create_endpoint("p1")  # type: ignore[attr-defined]
    assert isinstance(ep, FakeEndpoint)
    ep.queue_return(123)

    out = p.request("eth_chainId", (), route="primary")
    assert out == 123

    # before in order, after in reverse
    assert events == ["A:before", "B:before", "C:before", "C:after", "B:after", "A:after"]


def test_middleware_prepend_makes_outermost(_patch_endpoint) -> None:
    p = Provider([])
    p.set_primary("p1")

    events: list[str] = []
    p.add_middleware(RecorderMW(events, "B"))
    p.add_middleware(RecorderMW(events, "C"))
    p.add_middleware(RecorderMW(events, "A"), prepend=True)

    ep = p._get_or_create_endpoint("p1")  # type: ignore[attr-defined]
    ep.queue_return("ok")

    assert p.request("net_version", (), route="primary") == "ok"

    assert events == ["A:before", "B:before", "C:before", "C:after", "B:after", "A:after"]


def test_middleware_can_mutate_calls_single_and_results(_patch_endpoint) -> None:
    class MutateMW:
        def before_request(self, ctx, calls):
            # rewrite method name (toy)
            c = calls[0]
            calls[0] = _BatchCall(  # type: ignore[attr-defined]
                method="eth_chainId",
                params=c.params,
                formatter=c.formatter,
                freshness=c.freshness,
            )
            return calls

        def after_request(self, ctx, calls, results):
            # rewrite result
            results[0] = int(results[0]) + 1  # type: ignore[arg-type]
            return results

    p = Provider([])
    p.set_primary("p1")
    p.add_middleware(MutateMW())

    ep = p._get_or_create_endpoint("p1")  # type: ignore[attr-defined]
    ep.queue_return(41)

    out = p.request("net_version", (), route="primary")
    assert out == 42

    # Ensure it still used single-call Endpoint.request
    assert ep.calls[0][0] == "request"


def test_on_exception_can_recover_and_after_request_still_runs(_patch_endpoint) -> None:
    events: list[str] = []

    class RecoverMW:
        def before_request(self, ctx, calls):
            events.append("before")
            return calls

        def on_exception(self, ctx, calls, exc):
            events.append("exc")
            # Recover with a singleton list result aligned to calls
            return ["recovered"]

        def after_request(self, ctx, calls, results):
            events.append("after")
            return results

    p = Provider([])
    p.set_primary("p1")
    p.add_middleware(RecoverMW())

    ep = p._get_or_create_endpoint("p1")  # type: ignore[attr-defined]
    ep.queue_raise(TransportError("boom"))

    out = p.request("eth_chainId", (), route="primary")
    assert out == "recovered"
    assert events == ["before", "exc", "after"]


def test_global_default_middlewares_are_applied_at_provider_init(_patch_endpoint) -> None:
    events: list[str] = []

    class MW:
        def before_request(self, ctx, calls):
            events.append("before")
            return calls

    mw_mod.register_default_middleware(MW)

    p = Provider([])
    p.set_primary("p1")

    ep = p._get_or_create_endpoint("p1")  # type: ignore[attr-defined]
    ep.queue_return(1)

    assert p.request("eth_chainId", (), route="primary") == 1
    assert events == ["before"]


def test_global_default_factory_can_be_conditional(_patch_endpoint) -> None:
    events: list[str] = []

    class MW:
        def before_request(self, ctx, calls):
            events.append("before")
            return calls

    def factory(provider: Provider):
        # Only apply if a primary is already set (it isn't during Provider.__init__)
        if provider.has_primary():
            return MW()
        return None

    mw_mod.register_default_middleware(factory)

    p = Provider([])
    p.set_primary("p1")

    ep = p._get_or_create_endpoint("p1")  # type: ignore[attr-defined]
    ep.queue_return(1)

    assert p.request("eth_chainId", (), route="primary") == 1
    # factory ran at init when has_primary() was False, so it should not have installed
    assert events == []
