import time

from fw3.provider.pool import PoolManager
from fw3.provider.provider import Provider


class RecordingPoolManager:
    def __init__(self, urls: list[str]) -> None:
        self.urls = list(urls)
        self.calls: list[dict[str, object]] = []

    def best_urls(
        self,
        n: int,
        *,
        await_first: bool = False,
        exclude: set[str] | None = None,
    ) -> list[str]:
        exclude = exclude or set()
        self.calls.append(
            {
                "n": n,
                "await_first": await_first,
                "exclude": set(exclude),
            }
        )
        return [url for url in self.urls if url not in exclude][:n]


def test_pool_manager_best_urls_exclude_filters_and_backfills() -> None:
    pm = PoolManager(chain_id=1, target_pool=4)
    pm._active = [
        "http://a",
        "http://b",
        "http://c",
        "http://d",
    ]
    pm._active_set = set(pm._active)

    assert pm.best_urls(2, exclude={"http://a"}) == ["http://b", "http://c"]
    assert pm.best_urls(3, exclude={"http://a", "http://c"}) == [
        "http://b",
        "http://d",
    ]


def test_pool_manager_best_urls_exclude_can_underfill_when_reserve_is_missing() -> None:
    pm = PoolManager(chain_id=1, target_pool=2)
    pm._active = ["http://a", "http://b"]
    pm._active_set = set(pm._active)

    assert pm.best_urls(2, exclude={"http://a"}) == ["http://b"]


def test_provider_pool_candidates_excludes_cooldown_endpoint_and_uses_reserve() -> None:
    manager = RecordingPoolManager(["http://a", "http://b", "http://c"])
    provider = Provider(pool_manager=manager, desired_pool_size=2)

    cooled = provider._get_or_create_endpoint("http://a")
    provider._state[cooled].error_cooldown_until = time.time() + 60.0

    selected = provider._pool_candidates()

    assert [ep.target for ep in selected] == ["http://b", "http://c"]
    assert manager.calls == [
        {
            "n": 2,
            "await_first": True,
            "exclude": {"http://a"},
        }
    ]


def test_provider_pool_candidates_requests_only_missing_slots_from_manager() -> None:
    manager = RecordingPoolManager(["http://a", "http://b", "http://c"])
    provider = Provider(
        internal_endpoints=["http://internal-1"],
        pool_manager=manager,
        desired_pool_size=3,
    )

    selected = provider._pool_candidates()

    assert [ep.target for ep in selected] == [
        "http://internal-1",
        "http://a",
        "http://b",
    ]
    assert manager.calls == [
        {
            "n": 2,
            "await_first": False,
            "exclude": set(),
        }
    ]


def test_provider_pool_candidates_underfills_without_manager_reserve() -> None:
    manager = RecordingPoolManager(["http://a", "http://b"])
    provider = Provider(pool_manager=manager, desired_pool_size=2)

    cooled = provider._get_or_create_endpoint("http://a")
    provider._state[cooled].error_cooldown_until = time.time() + 60.0

    selected = provider._pool_candidates()

    assert [ep.target for ep in selected] == ["http://b"]
    assert manager.calls[0]["exclude"] == {"http://a"}
