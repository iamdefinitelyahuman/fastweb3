import pytest


@pytest.fixture(autouse=True)
def _stable_pool_probe_deadlines(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep pool probing fast *and* robust.

    Some integration tests patch probe deadlines very aggressively for speed.
    On slower machines, that can make the pool maintainer miss all probe results
    during its deadline window, leading to a "ready" pool with zero active URLs.

    A per-test override avoids that flakiness while staying much faster than the
    library defaults.
    """

    import fastweb3.rpc_pool as rpc_pool

    monkeypatch.setattr(rpc_pool, "PROBE_DEADLINE_MIN_S", 0.5)
    monkeypatch.setattr(rpc_pool, "PROBE_DEADLINE_MULTIPLIER", 0.0)
