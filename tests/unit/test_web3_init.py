import pytest

import fastweb3.web3.web3 as web3_mod
from fastweb3.errors import NoEndpoints


class FakePoolManager:
    def __init__(self, chain_id: int, **kwargs):
        self.chain_id = int(chain_id)
        self.kwargs = dict(kwargs)

    def best_urls(self, n: int) -> list[str]:
        return []


class DummyProvider:
    """
    Stand-in for Provider that captures constructor args and primary setting.
    """

    def __init__(
        self,
        internal_urls,
        *,
        pool_manager=None,
        desired_pool_size=6,
        retry_policy_pool=None,
        **kwargs,
    ):
        self.internal_urls = list(internal_urls)
        self.pool_manager = pool_manager
        self.desired_pool_size = desired_pool_size
        self.retry_policy_pool = retry_policy_pool
        self.kwargs = dict(kwargs)

        self._primary = None
        self._pin_calls = []
        self._request_calls = []

    def set_primary(self, url: str) -> None:
        self._primary = url

    def clear_primary(self) -> None:
        self._primary = None

    def primary_url(self):
        return self._primary

    def has_primary(self) -> bool:
        return self._primary is not None

    def urls(self) -> list[str]:
        # mirror Provider.urls() which returns internal pool snapshot
        return list(self.internal_urls)

    def request(self, method, params, *, route="pool", formatter=None):
        self._request_calls.append((method, list(params), route, formatter))
        raise RuntimeError("DummyProvider.request should not be called in __init__ tests")

    def pin(self, *, route="pool"):
        self._pin_calls.append(route)
        # context manager not needed for __init__ tests
        raise RuntimeError("DummyProvider.pin should not be called in __init__ tests")

    def close(self) -> None:
        return


@pytest.fixture
def patch_init_wiring(monkeypatch):
    created = {"pool_mgr_calls": [], "provider_insts": []}

    def fake_get_pool_manager(chain_id: int, **kwargs):
        created["pool_mgr_calls"].append((int(chain_id), dict(kwargs)))
        return FakePoolManager(chain_id, **kwargs)

    def fake_provider_ctor(*args, **kwargs):
        p = DummyProvider(*args, **kwargs)
        created["provider_insts"].append(p)
        return p

    monkeypatch.setattr(web3_mod, "get_pool_manager", fake_get_pool_manager)
    monkeypatch.setattr(web3_mod, "Provider", fake_provider_ctor)

    return created


def test_init_chain_id_only_uses_pool_manager(patch_init_wiring):
    created = patch_init_wiring

    w3 = web3_mod.Web3(1)

    # pool manager created once
    assert created["pool_mgr_calls"]
    chain_id, kwargs = created["pool_mgr_calls"][0]
    assert chain_id == 1
    # defaults from Web3.__init__ signature
    assert kwargs["target_pool"] == 6
    assert kwargs["max_lag_blocks"] == 8
    assert kwargs["probe_timeout_s"] == 1.5
    assert kwargs["probe_workers"] == 32

    # provider created with empty internal urls and pool_manager attached
    p = created["provider_insts"][-1]
    assert p.internal_urls == []
    assert p.pool_manager is not None
    assert p.primary_url() is None

    # eth namespace exists
    assert hasattr(w3, "eth")


def test_init_manual_endpoints_only_no_pool_manager(patch_init_wiring):
    created = patch_init_wiring

    w3 = web3_mod.Web3(endpoints=["https://a", "https://b"])

    # no pool manager for manual mode
    assert created["pool_mgr_calls"] == []

    p = created["provider_insts"][-1]
    assert p.pool_manager is None
    assert p.internal_urls == ["https://a", "https://b"]
    assert p.primary_url() is None
    assert hasattr(w3, "eth")


def test_init_primary_only(patch_init_wiring):
    created = patch_init_wiring

    w3 = web3_mod.Web3(primary_endpoint="http://localhost:8545")

    # no pool manager (no chain_id)
    assert created["pool_mgr_calls"] == []

    p = created["provider_insts"][-1]
    assert p.internal_urls == []
    assert p.primary_url() == "http://localhost:8545"
    assert hasattr(w3, "eth")


def test_init_chain_id_plus_primary_sets_primary_but_does_not_inject_into_internal_pool(
    patch_init_wiring,
):
    created = patch_init_wiring

    w3 = web3_mod.Web3(1, primary_endpoint="http://localhost:8545")

    # pool manager exists
    assert created["pool_mgr_calls"]
    p = created["provider_insts"][-1]

    # internal pool remains empty (hybrid primary does NOT auto join pool)
    assert p.internal_urls == []
    assert p.pool_manager is not None
    assert p.primary_url() == "http://localhost:8545"
    assert hasattr(w3, "eth")


def test_init_chain_id_plus_endpoints_plus_primary_hybrid(patch_init_wiring):
    created = patch_init_wiring

    w3 = web3_mod.Web3(
        1,
        endpoints=["https://user1", "https://user2"],
        primary_endpoint="http://localhost:8545",
    )

    assert created["pool_mgr_calls"]
    p = created["provider_insts"][-1]

    # internal pool = user endpoints, primary separate
    assert p.internal_urls == ["https://user1", "https://user2"]
    assert p.pool_manager is not None
    assert p.primary_url() == "http://localhost:8545"
    assert hasattr(w3, "eth")


def test_init_no_chain_no_endpoints_no_primary_raises(patch_init_wiring):
    with pytest.raises(NoEndpoints):
        web3_mod.Web3()
