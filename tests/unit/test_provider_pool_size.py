# tests/unit/test_provider_pool_size.py

import time

import fw3.provider.endpoint_selection as endpoint_selection_mod
from fw3.provider.provider import Provider


class FakeEndpoint:
    def __init__(self, target: str) -> None:
        self.target = target

    def close(self) -> None:
        pass


class FakePoolManager:
    def __init__(self, urls: list[str]) -> None:
        self.urls = list(urls)
        self.calls: list[tuple[object, bool, set[str] | None]] = []

    def best_urls(
        self,
        n: int | None,
        *,
        await_first: bool = False,
        exclude: set[str] | None = None,
    ) -> list[str]:
        exclude = set() if exclude is None else set(exclude)
        self.calls.append((n, await_first, set(exclude)))

        urls = [u for u in self.urls if u not in exclude]
        if n is None:
            return urls
        return urls[:n]


def _cooldown_endpoint(p: Provider, target: str, *, seconds: float = 60.0) -> None:
    ep = p._eps_by_target[target]
    st = p._state[ep]
    st.error_cooldown_until = time.time() + seconds


def test_pool_size_counts_active_internal_and_backfills_from_pool_manager(monkeypatch) -> None:
    monkeypatch.setattr(endpoint_selection_mod, "Endpoint", FakeEndpoint)

    pm = FakePoolManager(
        [
            "http://pm-1",
            "http://pm-2",
            "http://pm-3",
        ]
    )
    p = Provider(
        ["http://internal-1", "http://internal-2"],
        pool_manager=pm,
        desired_pool_size=3,
    )

    assert p.pool_size() == 3
    assert pm.calls[-1] == (1, False, set())

    _cooldown_endpoint(p, "http://internal-1")

    assert p.pool_size() == 3
    assert pm.calls[-1] == (2, False, {"http://internal-1"})


def test_pool_size_does_not_use_pool_manager_when_active_internal_already_fill_pool(
    monkeypatch,
) -> None:
    monkeypatch.setattr(endpoint_selection_mod, "Endpoint", FakeEndpoint)

    pm = FakePoolManager(["http://pm-1", "http://pm-2"])
    p = Provider(
        ["http://internal-1", "http://internal-2"],
        pool_manager=pm,
        desired_pool_size=2,
    )

    assert p.pool_size() == 2
    assert pm.calls == []

    _cooldown_endpoint(p, "http://internal-2")

    assert p.pool_size() == 2
    assert pm.calls == [(1, False, {"http://internal-2"})]


def test_pool_capacity_includes_all_internal_and_all_pool_manager_urls_and_ignores_cooldown(
    monkeypatch,
) -> None:
    monkeypatch.setattr(endpoint_selection_mod, "Endpoint", FakeEndpoint)

    pm = FakePoolManager(
        [
            "http://internal-2",
            "http://pm-1",
            "http://pm-2",
        ]
    )
    p = Provider(
        ["http://internal-1", "http://internal-2"],
        pool_manager=pm,
        desired_pool_size=3,
    )

    _cooldown_endpoint(p, "http://internal-1")
    _cooldown_endpoint(p, "http://internal-2")

    assert p.pool_capacity() == 4
    assert pm.calls[-1] == (None, False, set())


def test_pool_capacity_reflects_pool_manager_changes_but_pool_size_still_respects_desired_pool_size(
    monkeypatch,
) -> None:
    monkeypatch.setattr(endpoint_selection_mod, "Endpoint", FakeEndpoint)

    pm = FakePoolManager(["http://pm-1"])
    p = Provider(
        ["http://internal-1"],
        pool_manager=pm,
        desired_pool_size=2,
    )

    assert p.pool_size() == 2
    assert p.pool_capacity() == 2

    pm.urls.append("http://pm-2")
    pm.urls.append("http://pm-3")

    assert p.pool_size() == 2
    assert p.pool_capacity() == 4


def test_pool_size_and_pool_capacity_are_internal_only_without_pool_manager(monkeypatch) -> None:
    monkeypatch.setattr(endpoint_selection_mod, "Endpoint", FakeEndpoint)

    p = Provider(["http://internal-1", "http://internal-2"])

    assert p.pool_size() == 2
    assert p.pool_capacity() == 2

    _cooldown_endpoint(p, "http://internal-2")

    assert p.pool_size() == 1
    assert p.pool_capacity() == 2
