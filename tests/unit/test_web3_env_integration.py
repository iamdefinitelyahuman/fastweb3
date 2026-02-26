# tests/unit/test_web3_env_integration.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

import fastweb3.web3 as w3mod
from fastweb3.errors import NoEndpoints


@dataclass
class _ProviderInit:
    internal_urls: list[str]
    pool_manager: Any
    desired_pool_size: int
    retry_policy_pool: Any


class FakeProvider:
    """
    Spy Provider that records ctor args + set_primary calls.
    """

    def __init__(
        self,
        internal_urls: list[str],
        *,
        pool_manager: Any = None,
        desired_pool_size: int,
        retry_policy_pool: Any,
    ) -> None:
        self.init = _ProviderInit(
            internal_urls=list(internal_urls),
            pool_manager=pool_manager,
            desired_pool_size=desired_pool_size,
            retry_policy_pool=retry_policy_pool,
        )
        self.primary_set: list[str] = []

    def set_primary(self, url: str) -> None:
        self.primary_set.append(url)

    def close(self) -> None:
        pass

    # Web3.make_request uses provider.request; these tests never call make_request
    def request(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("request() should not be called in these wiring tests")

    # Web3.pin calls provider.pin; not used here
    def pin(self, *args: Any, **kwargs: Any):  # pragma: no cover
        raise AssertionError("pin() should not be called in these wiring tests")


class FakeEndpoint:
    """
    Spy Endpoint used only for _get_default_primary_chain_id_once probe.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self.closed = False

    def request(self, method: str, params: Any, *, formatter=None) -> Any:
        raise AssertionError("FakeEndpoint.request must be monkeypatched per-test")

    def close(self) -> None:
        self.closed = True


def _reset_default_primary_chain_id_cache() -> None:
    # Reset module-level cache so each test is independent
    w3mod._DEFAULT_PRIMARY_CHAIN_ID_SET = False
    w3mod._DEFAULT_PRIMARY_CHAIN_ID = None


def test_explicit_primary_endpoint_overrides_env_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINT", "http://env-primary")
    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    # pool manager wiring irrelevant; keep it simple and avoid probe
    monkeypatch.setattr(w3mod, "get_pool_manager", lambda *a, **k: object())

    w3 = w3mod.Web3(1, primary_endpoint="http://explicit-primary")

    assert isinstance(w3.provider, FakeProvider)
    # Explicit always wins; env primary should not be applied
    assert w3.provider.primary_set == ["http://explicit-primary"]


def test_pool_mode_off_never_calls_get_pool_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "off")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    called = {"n": 0}

    def _boom(*args: Any, **kwargs: Any) -> Any:
        called["n"] += 1
        raise AssertionError("get_pool_manager should not be called when POOL_MODE=off")

    monkeypatch.setattr(w3mod, "get_pool_manager", _boom)

    w3 = w3mod.Web3(1)

    assert isinstance(w3.provider, FakeProvider)
    assert called["n"] == 0
    assert w3.provider.init.pool_manager is None


def test_pool_mode_default_calls_get_pool_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    sentinel = object()
    seen: dict[str, Any] = {}

    def _get_pool_manager(chain_id: int, **kwargs: Any) -> Any:
        seen["chain_id"] = chain_id
        seen["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(w3mod, "get_pool_manager", _get_pool_manager)

    w3 = w3mod.Web3(10, target_pool=7, max_lag_blocks=9, probe_timeout_s=0.7, probe_workers=11)

    assert isinstance(w3.provider, FakeProvider)
    assert w3.provider.init.pool_manager is sentinel
    assert seen["chain_id"] == 10
    # sanity: knobs passed through
    assert seen["kwargs"]["target_pool"] == 7
    assert seen["kwargs"]["max_lag_blocks"] == 9
    assert seen["kwargs"]["probe_timeout_s"] == 0.7
    assert seen["kwargs"]["probe_workers"] == 11


def test_split_per_chain_primary_disables_pool_for_that_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "split")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINTS", "10=http://ten-primary")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    called = {"n": 0}

    def _boom(*args: Any, **kwargs: Any) -> Any:
        called["n"] += 1
        raise AssertionError(
            "get_pool_manager should not be called for chain with per-chain primary in split mode"
        )

    monkeypatch.setattr(w3mod, "get_pool_manager", _boom)

    w3 = w3mod.Web3(10)

    assert isinstance(w3.provider, FakeProvider)
    assert called["n"] == 0
    assert w3.provider.init.pool_manager is None
    # env primary applied
    assert w3.provider.primary_set == ["http://ten-primary"]


def test_split_per_chain_primary_still_uses_pool_on_other_chains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "split")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINTS", "10=http://ten-primary")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    sentinel = object()
    monkeypatch.setattr(w3mod, "get_pool_manager", lambda *a, **k: sentinel)

    w3 = w3mod.Web3(8453)

    assert isinstance(w3.provider, FakeProvider)
    assert w3.provider.init.pool_manager is sentinel
    # no primary for this chain, so none applied
    assert w3.provider.primary_set == []


def test_split_global_primary_disables_pool_only_on_that_primary_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "split")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINT", "http://global-primary")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)

    # Ensure probing uses our fake endpoint and returns chain id = 1
    monkeypatch.setattr(w3mod, "Endpoint", FakeEndpoint)

    def _endpoint_request(self: FakeEndpoint, method: str, params: Any, *, formatter=None) -> Any:
        assert method == "eth_chainId"
        # Web3 probes with formatter=to_int; return a hex string so formatter is exercised
        raw = "0x1"
        return formatter(raw) if formatter is not None else raw

    monkeypatch.setattr(FakeEndpoint, "request", _endpoint_request)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    # On chain 1, pool should be disabled (because global primary is on chain 1)
    called = {"n": 0}

    def _boom(*args: Any, **kwargs: Any) -> Any:
        called["n"] += 1
        raise AssertionError(
            "get_pool_manager should not be called on the global-primary chain in split mode"
        )

    monkeypatch.setattr(w3mod, "get_pool_manager", _boom)

    w3_chain1 = w3mod.Web3(1)
    assert isinstance(w3_chain1.provider, FakeProvider)
    assert called["n"] == 0
    assert w3_chain1.provider.init.pool_manager is None
    assert w3_chain1.provider.primary_set == ["http://global-primary"]

    # On another chain, pool should still be used, and global primary should NOT be applied
    sentinel = object()
    monkeypatch.setattr(w3mod, "get_pool_manager", lambda *a, **k: sentinel)

    w3_chain10 = w3mod.Web3(10)
    assert isinstance(w3_chain10.provider, FakeProvider)
    assert w3_chain10.provider.init.pool_manager is sentinel
    assert w3_chain10.provider.primary_set == []


def test_no_endpoints_error_mentions_pool_mode_off(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "off")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINT", raising=False)
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    with pytest.raises(NoEndpoints, match=r"pool disabled by FASTWEB3_POOL_MODE=off"):
        w3mod.Web3()  # no chain_id, no endpoints, no env primary


def test_env_default_primary_allows_primary_only_mode_without_chain_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINT", "http://env-primary")
    monkeypatch.delenv("FASTWEB3_PRIMARY_ENDPOINTS", raising=False)

    monkeypatch.setattr(w3mod, "Provider", FakeProvider)

    w3 = w3mod.Web3()  # no chain_id, no endpoints, should still work due to env primary

    assert isinstance(w3.provider, FakeProvider)
    assert w3.provider.init.internal_urls == []
    assert w3.provider.primary_set == ["http://env-primary"]


def test_explicit_provider_bypasses_env_and_pool_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_default_primary_chain_id_cache()

    monkeypatch.setenv("FASTWEB3_POOL_MODE", "default")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINT", "http://env-primary")
    monkeypatch.setenv("FASTWEB3_PRIMARY_ENDPOINTS", "1=http://per-chain")

    # If Web3 tries to build anything, fail the test
    monkeypatch.setattr(
        w3mod,
        "Provider",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("Provider should not be constructed")),
    )
    monkeypatch.setattr(
        w3mod,
        "get_pool_manager",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("get_pool_manager should not be called")
        ),
    )

    explicit = FakeProvider([], pool_manager=None, desired_pool_size=6, retry_policy_pool=object())

    w3 = w3mod.Web3(1, provider=explicit, primary_endpoint=None)

    assert w3.provider is explicit
    # env shouldn't be applied when provider is explicitly supplied
    assert explicit.primary_set == []
