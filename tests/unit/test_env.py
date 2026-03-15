# tests/unit/test_env.py
from __future__ import annotations

import pytest

from fw3.env import (
    get_default_primary_endpoint,
    get_use_public_pool,
    parse_primary_endpoints,
    resolve_primary_endpoint,
    should_use_pool,
)


def test_get_use_public_pool_none_when_unset_or_empty() -> None:
    assert get_use_public_pool({}) is None
    assert get_use_public_pool({"FASTWEB3_USE_PUBLIC_POOL": ""}) is None
    assert get_use_public_pool({"FASTWEB3_USE_PUBLIC_POOL": "   "}) is None


def test_get_use_public_pool_parses_true_and_false() -> None:
    for raw in ("true", "1", "yes", "on", " TRUE ", " On "):
        assert get_use_public_pool({"FASTWEB3_USE_PUBLIC_POOL": raw}) is True

    for raw in ("false", "0", "no", "off", " FALSE ", " Off "):
        assert get_use_public_pool({"FASTWEB3_USE_PUBLIC_POOL": raw}) is False


def test_get_use_public_pool_rejects_invalid() -> None:
    with pytest.raises(ValueError, match="FASTWEB3_USE_PUBLIC_POOL"):
        get_use_public_pool({"FASTWEB3_USE_PUBLIC_POOL": "nope"})


def test_get_default_primary_endpoint_none_when_unset_or_empty() -> None:
    assert get_default_primary_endpoint({}) is None
    assert get_default_primary_endpoint({"FASTWEB3_PRIMARY_ENDPOINT": ""}) is None
    assert get_default_primary_endpoint({"FASTWEB3_PRIMARY_ENDPOINT": "   "}) is None


def test_get_default_primary_endpoint_strips() -> None:
    assert get_default_primary_endpoint({"FASTWEB3_PRIMARY_ENDPOINT": "  http://x  "}) == "http://x"


def test_parse_primary_endpoints_empty() -> None:
    assert parse_primary_endpoints({}) == {}
    assert parse_primary_endpoints({"FASTWEB3_PRIMARY_ENDPOINTS": ""}) == {}
    assert parse_primary_endpoints({"FASTWEB3_PRIMARY_ENDPOINTS": "   "}) == {}


def test_parse_primary_endpoints_parses_and_strips() -> None:
    env = {"FASTWEB3_PRIMARY_ENDPOINTS": " 1 = http://a ; 10=https://b ;8453=  http://c "}
    out = parse_primary_endpoints(env)
    assert out == {1: "http://a", 10: "https://b", 8453: "http://c"}


def test_parse_primary_endpoints_last_wins_on_duplicate_chain() -> None:
    env = {"FASTWEB3_PRIMARY_ENDPOINTS": "1=http://a;1=http://b"}
    assert parse_primary_endpoints(env) == {1: "http://b"}


def test_parse_primary_endpoints_ignores_empty_segments() -> None:
    env = {"FASTWEB3_PRIMARY_ENDPOINTS": "1=http://a;;  ;10=http://b;"}
    assert parse_primary_endpoints(env) == {1: "http://a", 10: "http://b"}


def test_parse_primary_endpoints_rejects_missing_equals() -> None:
    with pytest.raises(ValueError, match="entry must be 'chainid=url'"):
        parse_primary_endpoints({"FASTWEB3_PRIMARY_ENDPOINTS": "1http://a"})


def test_parse_primary_endpoints_rejects_empty_key_or_value() -> None:
    with pytest.raises(ValueError, match="entry must be 'chainid=url'"):
        parse_primary_endpoints({"FASTWEB3_PRIMARY_ENDPOINTS": "=http://a"})
    with pytest.raises(ValueError, match="entry must be 'chainid=url'"):
        parse_primary_endpoints({"FASTWEB3_PRIMARY_ENDPOINTS": "1="})


def test_parse_primary_endpoints_rejects_non_int_chainid() -> None:
    with pytest.raises(ValueError, match="chainid must be an int"):
        parse_primary_endpoints({"FASTWEB3_PRIMARY_ENDPOINTS": "one=http://a"})


def test_resolve_primary_endpoint_prefers_per_chain_over_default() -> None:
    env = {
        "FASTWEB3_PRIMARY_ENDPOINT": "http://default",
        "FASTWEB3_PRIMARY_ENDPOINTS": "1=http://one;10=http://ten",
    }
    assert resolve_primary_endpoint(1, env=env) == "http://one"
    assert resolve_primary_endpoint(10, env=env) == "http://ten"
    assert resolve_primary_endpoint(8453, env=env) == "http://default"


def test_resolve_primary_endpoint_none_when_nothing_set() -> None:
    assert resolve_primary_endpoint(1, env={}) is None


def test_resolve_primary_endpoint_filters_default_primary_when_chain_id_known() -> None:
    env = {
        "FASTWEB3_PRIMARY_ENDPOINT": "http://default",
    }
    # Global primary only applies on its own chain when default_primary_chain_id is provided.
    assert resolve_primary_endpoint(1, env=env, default_primary_chain_id=1) == "http://default"
    assert resolve_primary_endpoint(10, env=env, default_primary_chain_id=1) is None


def test_resolve_primary_endpoint_does_not_filter_per_chain_mapping() -> None:
    env = {
        "FASTWEB3_PRIMARY_ENDPOINT": "http://default",
        "FASTWEB3_PRIMARY_ENDPOINTS": "10=http://ten",
    }
    assert resolve_primary_endpoint(10, env=env, default_primary_chain_id=1) == "http://ten"


def test_should_use_pool_default_when_env_unset_or_true() -> None:
    assert should_use_pool(1, env={}) is True
    assert should_use_pool(1, env={"FASTWEB3_USE_PUBLIC_POOL": "true"}) is True


def test_should_use_pool_false_without_primary_still_uses_pool() -> None:
    env = {"FASTWEB3_USE_PUBLIC_POOL": "false"}
    assert should_use_pool(1, env=env) is True
    assert should_use_pool(10, env=env) is True


def test_should_use_pool_false_disables_pool_only_when_primary_for_chain_exists() -> None:
    env = {
        "FASTWEB3_USE_PUBLIC_POOL": "false",
        "FASTWEB3_PRIMARY_ENDPOINT": "http://default",
        "FASTWEB3_PRIMARY_ENDPOINTS": "10=http://ten",
    }

    # With default_primary_chain_id=1:
    # - chain 1 has a configured primary (global), so pool disabled
    # - chain 10 has a configured primary (per-chain), so pool disabled
    # - chain 8453 has no configured primary, so pool remains enabled
    assert should_use_pool(1, env=env, default_primary_chain_id=1) is False
    assert should_use_pool(10, env=env, default_primary_chain_id=1) is False
    assert should_use_pool(8453, env=env, default_primary_chain_id=1) is True
