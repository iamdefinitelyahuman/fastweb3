# tests/unit/test_web3_batching.py
import threading
from typing import Any, Callable

import pytest

from fw3.errors import RPCError, RPCErrorDetails
from fw3.web3.web3 import Web3


class FakeProvider:
    def __init__(self) -> None:
        self.request_calls: list[tuple[str, list[Any], str]] = []
        self.request_batch_calls: list[tuple[list[tuple[Any, ...]], str]] = []
        self._batch_responses: list[list[Any | RPCError]] = []
        self._request_fn: Callable[[str, list[Any], str], Any] | None = None

    def set_request_fn(self, fn: Callable[[str, list[Any], str], Any]) -> None:
        self._request_fn = fn

    def queue_batch_response(self, resp: list[Any | RPCError]) -> None:
        self._batch_responses.append(resp)

    def request(
        self,
        method: str,
        params: list[Any],
        *,
        route: str = "pool",
        formatter=None,
        freshness=None,
    ):
        self.request_calls.append((method, list(params), route))
        if self._request_fn is not None:
            return self._request_fn(method, list(params), route)
        return (method, tuple(params), route)

    def request_batch(self, calls, *, route: str = "pool"):
        self.request_batch_calls.append((list(calls), route))
        if not self._batch_responses:
            out: list[Any | RPCError] = []
            for c in calls:
                out.append(c[0])
            return out
        return self._batch_responses.pop(0)

    def close(self) -> None:
        pass


def _rpc_err(code: int = 3, message: str = "execution reverted") -> RPCError:
    return RPCError(RPCErrorDetails(code=code, message=message, data=None))


def test_batch_queues_and_flushes_on_proxy_interaction() -> None:
    p = FakeProvider()
    p.queue_batch_response([111, 222])

    w3 = Web3(provider=p)

    with w3.batch_requests() as batch:
        x = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)
        y = w3.make_request("eth_getBalance", ["0xabc", "latest"], formatter=int)

        assert batch.pending_count() == 2
        assert p.request_calls == []
        assert p.request_batch_calls == []

        # Any interaction should resolve and flush.
        assert int(x) == 111

    assert int(y) == 222
    assert len(p.request_batch_calls) == 1


def test_batch_flushes_on_exit_if_no_proxy_interaction() -> None:
    p = FakeProvider()
    p.queue_batch_response([1, 2])

    w3 = Web3(provider=p)

    with w3.batch_requests():
        x = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)
        y = w3.make_request("eth_getBalance", ["0xabc", "latest"], formatter=int)
        assert p.request_batch_calls == []

    assert len(p.request_batch_calls) == 1
    assert int(x) == 1
    assert int(y) == 2


def test_never_batch_methods_execute_immediately() -> None:
    p = FakeProvider()
    w3 = Web3(provider=p)

    with w3.batch_requests():
        tx = w3.make_request("eth_sendRawTransaction", ["0xdeadbeef"], formatter=str)
        assert str(tx) == "('eth_sendRawTransaction', ('0xdeadbeef',), 'pool')"

    assert len(p.request_calls) == 1
    assert len(p.request_batch_calls) == 0


def test_methods_filter_only_batches_selected_methods() -> None:
    p = FakeProvider()
    p.queue_batch_response([10])

    w3 = Web3(provider=p)

    with w3.batch_requests(methods={"eth_call"}):
        x = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)
        y = w3.make_request("net_version", [], formatter=str)

        # net_version should NOT be enqueued; it should execute immediately.
        assert str(y) == "('net_version', (), 'pool')"
        assert len(p.request_calls) == 1
        assert p.request_calls[0][0] == "net_version"

        assert int(x) == 10

    assert len(p.request_batch_calls) == 1
    assert len(p.request_batch_calls[0][0]) == 1
    assert p.request_batch_calls[0][0][0][0] == "eth_call"


def test_nested_batches_create_boundaries() -> None:
    p = FakeProvider()
    p.queue_batch_response([1])  # outer before inner
    p.queue_batch_response([2])  # inner
    p.queue_batch_response([3])  # outer after inner

    w3 = Web3(provider=p)

    with w3.batch_requests():
        a = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)

        with w3.batch_requests(methods={"eth_call"}):
            b = w3.make_request("eth_call", [{"to": "0x1"}], formatter=int)

        c = w3.make_request("eth_call", [{"to": "0x2"}], formatter=int)

    assert int(a) == 1
    assert int(b) == 2
    assert int(c) == 3
    assert len(p.request_batch_calls) == 3


def test_flush_raises_if_any_call_errors() -> None:
    p = FakeProvider()
    p.queue_batch_response([_rpc_err(), 5])

    w3 = Web3(provider=p)

    with w3.batch_requests():
        x = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)
        y = w3.make_request("eth_call", [{"to": "0x1"}], formatter=int)

        with pytest.raises(RPCError):
            str(x)  # triggers flush

        # y should still be resolved even though flush raised
        assert int(y) == 5


def test_exit_with_exception_does_not_mask_original_exception() -> None:
    p = FakeProvider()
    p.queue_batch_response([7])

    w3 = Web3(provider=p)

    x = None
    try:
        with w3.batch_requests():
            x = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)
            _ = 33 + "potato"  # type: ignore[operator]
    except TypeError:
        pass

    assert int(x) == 7
    assert len(p.request_batch_calls) == 1


def test_batch_is_thread_local() -> None:
    p = FakeProvider()
    p.queue_batch_response([123])

    w3 = Web3(provider=p)

    results: dict[str, Any] = {}

    def worker() -> None:
        with w3.batch_requests():
            x = w3.make_request("eth_call", [{"to": "0x0"}], formatter=int)
            results["x"] = int(x)

    t = threading.Thread(target=worker)
    t.start()

    # Main thread should not be batched (no active context here)
    y = w3.make_request("eth_call", [{"to": "0x1"}], formatter=str)
    results["y"] = str(y)

    t.join()

    assert results["x"] == 123
    assert "eth_call" in results["y"]
    assert len(p.request_batch_calls) == 1
    assert len(p.request_calls) == 1
