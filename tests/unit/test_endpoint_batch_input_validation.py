from __future__ import annotations

from typing import Any

import pytest

from fastweb3.endpoint import Endpoint


class NoSendTransport:
    """Transport that fails if Endpoint tries to send a request."""

    def send(self, payload: Any) -> Any:  # pragma: no cover
        raise AssertionError(f"send() should not be called for this test, got: {payload!r}")

    def close(self) -> None:
        pass


def test_request_batch_empty_returns_empty_list_without_sending() -> None:
    e = Endpoint("https://example.invalid", transport=NoSendTransport())
    assert e.request_batch() == []


def test_request_batch_rejects_non_tuple_call_before_sending() -> None:
    e = Endpoint("https://example.invalid", transport=NoSendTransport())
    with pytest.raises(TypeError, match="must be a tuple"):
        e.request_batch("eth_chainId")  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_call", [("m",), ("m", (), None, None)])
def test_request_batch_rejects_wrong_tuple_length_before_sending(bad_call: Any) -> None:
    e = Endpoint("https://example.invalid", transport=NoSendTransport())
    with pytest.raises(TypeError, match="Batch call must be"):
        e.request_batch(bad_call)  # type: ignore[arg-type]
