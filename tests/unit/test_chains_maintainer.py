import fastweb3.chains as chains

# -------------------------
# Fake Provider
# -------------------------


class FakeProvider:
    def __init__(self, urls):
        self._urls = list(urls)

    def urls(self):
        return list(self._urls)

    def endpoint_count(self):
        return len(self._urls)

    def add_url(self, u):
        if u not in self._urls:
            self._urls.append(u)

    def remove_url(self, u):
        self._urls = [x for x in self._urls if x != u]


def pr(url, rtt, head):
    return chains.ProbeResult(url=url, rtt_ms=rtt, head=head)


# -------------------------
# Fill behavior
# -------------------------


def test_fill_until_target_pool():
    provider = FakeProvider([])
    state = chains.MaintainerState()
    pinned = set()

    chains._maintainer_handle_probe_result(
        state=state,
        provider=provider,
        pr=pr("https://a", 100, 10),
        pinned=pinned,
        target_pool=2,
        max_lag_blocks=8,
        now=0,
    )

    chains._maintainer_handle_probe_result(
        state=state,
        provider=provider,
        pr=pr("https://b", 90, 11),
        pinned=pinned,
        target_pool=2,
        max_lag_blocks=8,
        now=0,
    )

    assert sorted(provider.urls()) == ["https://a", "https://b"]


# -------------------------
# Replace slowest
# -------------------------


def test_replace_slowest_non_pinned():
    provider = FakeProvider(["https://slow", "https://fast"])
    state = chains.MaintainerState(
        rtt_by_url={
            "https://slow": 200,
            "https://fast": 80,
        }
    )
    pinned = set()

    chains._maintainer_handle_probe_result(
        state=state,
        provider=provider,
        pr=pr("https://new", 50, 20),
        pinned=pinned,
        target_pool=2,
        max_lag_blocks=8,
        now=0,
    )

    assert "https://slow" not in provider.urls()
    assert "https://new" in provider.urls()


# -------------------------
# Pinned endpoints never evicted
# -------------------------


def test_pinned_not_replaced():
    provider = FakeProvider(["https://pinned", "https://slow"])
    state = chains.MaintainerState(
        rtt_by_url={
            "https://pinned": 300,
            "https://slow": 200,
        }
    )
    pinned = {"https://pinned"}

    chains._maintainer_handle_probe_result(
        state=state,
        provider=provider,
        pr=pr("https://new", 50, 10),
        pinned=pinned,
        target_pool=2,
        max_lag_blocks=8,
        now=0,
    )

    assert "https://pinned" in provider.urls()
    assert "https://slow" not in provider.urls()


# -------------------------
# Replace cooldown respected
# -------------------------


def test_replace_cooldown_blocks_replacement():
    provider = FakeProvider(["https://slow", "https://fast"])
    state = chains.MaintainerState(
        rtt_by_url={
            "https://slow": 200,
            "https://fast": 80,
        },
        next_replace_ts=100,
    )
    pinned = set()

    chains._maintainer_handle_probe_result(
        state=state,
        provider=provider,
        pr=pr("https://new", 50, 20),
        pinned=pinned,
        target_pool=2,
        max_lag_blocks=8,
        now=0,
    )

    assert "https://slow" in provider.urls()
    assert "https://new" not in provider.urls()


# -------------------------
# Lag eviction
# -------------------------


def test_lagging_endpoint_evicted(monkeypatch):
    provider = FakeProvider(["https://a"])
    state = chains.MaintainerState(best_head=100)
    pinned = set()

    def fake_probe_one(url, **kwargs):
        return pr(url, 100, 80)  # lagging by 20

    monkeypatch.setattr(chains, "_probe_one", fake_probe_one)

    chains._maintainer_health_check(
        state=state,
        provider=provider,
        pinned=pinned,
        chain_id=1,
        max_lag_blocks=5,
        probe_timeout_s=0.1,
        now=0,
    )

    assert provider.urls() == []


# -------------------------
# Failure eviction
# -------------------------


def test_failed_endpoint_evicted(monkeypatch):
    provider = FakeProvider(["https://a"])
    state = chains.MaintainerState()
    pinned = set()

    def fake_probe_one(url, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(chains, "_probe_one", fake_probe_one)

    chains._maintainer_health_check(
        state=state,
        provider=provider,
        pinned=pinned,
        chain_id=1,
        max_lag_blocks=8,
        probe_timeout_s=0.1,
        now=0,
    )

    assert provider.urls() == []


# -------------------------
# Eviction cooldown respected
# -------------------------


def test_eviction_cooldown_blocks_readd():
    provider = FakeProvider([])
    state = chains.MaintainerState(cooldown_until={"https://a": 100})
    pinned = set()

    chains._maintainer_handle_probe_result(
        state=state,
        provider=provider,
        pr=pr("https://a", 50, 10),
        pinned=pinned,
        target_pool=2,
        max_lag_blocks=8,
        now=0,
    )

    assert provider.urls() == []
