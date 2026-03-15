from __future__ import annotations

from dataclasses import dataclass

from fw3.web3 import web3 as web3_mod


class DummyProvider:
    def __init__(
        self,
        internal_endpoints,
        *,
        pool_manager,
        desired_pool_size,
        retry_policy_pool,
    ) -> None:
        self.internal_endpoints = list(internal_endpoints)
        self.pool_manager = pool_manager
        self.desired_pool_size = desired_pool_size
        self.retry_policy_pool = retry_policy_pool
        self.primary = None

    def set_primary(self, target: str) -> None:
        self.primary = target

    def close(self) -> None:
        return None


@dataclass
class AcquireCall:
    chain_id: int
    target_pool: int
    max_lag_blocks: int


class AcquireRecorder:
    def __init__(self) -> None:
        self.calls: list[AcquireCall] = []

    def __call__(self, chain_id: int, *, target_pool: int, max_lag_blocks: int):
        self.calls.append(
            AcquireCall(
                chain_id=chain_id,
                target_pool=target_pool,
                max_lag_blocks=max_lag_blocks,
            )
        )
        return object()


def _patch_web3_dependencies(monkeypatch, acquire_recorder: AcquireRecorder) -> None:
    monkeypatch.setattr(web3_mod, "Provider", DummyProvider)
    monkeypatch.setattr(web3_mod, "Eth", lambda w3: object())
    monkeypatch.setattr(web3_mod, "acquire_pool_manager", acquire_recorder)
    monkeypatch.setattr(
        web3_mod, "should_use_pool", lambda chain_id, default_primary_chain_id=None: True
    )
    monkeypatch.setattr(web3_mod, "get_default_primary_endpoint", lambda: None)
    monkeypatch.setattr(
        web3_mod,
        "resolve_primary_endpoint",
        lambda chain_id, default_primary_chain_id=None: None,
    )


def test_web3_uses_larger_pool_manager_target_than_provider_pool(monkeypatch) -> None:
    acquire_recorder = AcquireRecorder()
    _patch_web3_dependencies(monkeypatch, acquire_recorder)

    cfg = web3_mod.Web3Config(desired_pool_size=6, max_lag_blocks=11)
    w3 = web3_mod.Web3(chain_id=1, config=cfg)

    assert acquire_recorder.calls == [AcquireCall(chain_id=1, target_pool=12, max_lag_blocks=11)]
    assert w3.provider.desired_pool_size == 6
    assert w3.provider.pool_manager is not None


def test_web3_pool_manager_target_has_plus_three_floor_for_small_sizes(monkeypatch) -> None:
    acquire_recorder = AcquireRecorder()
    _patch_web3_dependencies(monkeypatch, acquire_recorder)

    cfg = web3_mod.Web3Config(desired_pool_size=1, max_lag_blocks=8)
    w3 = web3_mod.Web3(chain_id=1, config=cfg)

    assert acquire_recorder.calls == [AcquireCall(chain_id=1, target_pool=4, max_lag_blocks=8)]
    assert w3.provider.desired_pool_size == 1


def test_web3_provider_still_uses_exact_desired_pool_size_when_manager_tracks_more(
    monkeypatch,
) -> None:
    acquire_recorder = AcquireRecorder()
    _patch_web3_dependencies(monkeypatch, acquire_recorder)

    cfg = web3_mod.Web3Config(desired_pool_size=3, max_lag_blocks=5)
    w3 = web3_mod.Web3(chain_id=1, endpoints=["http://internal"], config=cfg)

    assert acquire_recorder.calls == [AcquireCall(chain_id=1, target_pool=6, max_lag_blocks=5)]
    assert w3.provider.desired_pool_size == 3
    assert w3.provider.internal_endpoints == ["http://internal"]
