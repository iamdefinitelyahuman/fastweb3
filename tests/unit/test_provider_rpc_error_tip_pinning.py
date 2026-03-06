from __future__ import annotations

from fastweb3.provider.rpc_error_retry import _pin_calls_to_tip
from fastweb3.provider.types import _BatchCall


def test_pin_calls_to_tip_replaces_latest_in_second_position_and_preserves_container_type() -> None:
    tuple_call = _BatchCall("eth_call", ({"to": "0x1"}, "latest"))
    list_call = _BatchCall("eth_getBalance", ["0xabc", "latest"])

    out = _pin_calls_to_tip([tuple_call, list_call], 255)

    assert out[0].params == ({"to": "0x1"}, "0xff")
    assert isinstance(out[0].params, tuple)
    assert out[1].params == ["0xabc", "0xff"]
    assert isinstance(out[1].params, list)


def test_pin_calls_to_tip_appends_missing_second_position_block_arg() -> None:
    call = _BatchCall("eth_getCode", ["0xabc"])
    out = _pin_calls_to_tip([call], 16)
    assert out[0].params == ["0xabc", "0x10"]


def test_pin_calls_to_tip_replaces_latest_in_first_position_methods() -> None:
    call = _BatchCall("eth_getBlockByNumber", ["latest", False])
    out = _pin_calls_to_tip([call], 32)
    assert out[0].params == ["0x20", False]


def test_pin_calls_to_tip_does_not_touch_explicit_block_tags_or_unrelated_methods() -> None:
    calls = [
        _BatchCall("eth_call", [{"to": "0x1"}, "pending"]),
        _BatchCall("eth_call", [{"to": "0x1"}, "0x123"]),
        _BatchCall("net_version", []),
    ]

    out = _pin_calls_to_tip(calls, 99)

    assert out[0].params == [{"to": "0x1"}, "pending"]
    assert out[1].params == [{"to": "0x1"}, "0x123"]
    assert out[2].params == []


def test_pin_calls_to_tip_returns_new_calls_without_mutating_original() -> None:
    original = _BatchCall("eth_call", [{"to": "0x1"}, "latest"])
    out = _pin_calls_to_tip([original], 15)

    assert out[0] is not original
    assert original.params == [{"to": "0x1"}, "latest"]
    assert out[0].params == [{"to": "0x1"}, "0xf"]
