import pytest

from fw3.utils import is_url_target, normalize_target, normalize_url


def test_normalize_url_expands_dollar_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INFURA_KEY", "abc123")
    out = normalize_url(" https://mainnet.infura.io/v3/$INFURA_KEY ")
    assert out == "https://mainnet.infura.io/v3/abc123"


def test_normalize_url_expands_braced_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALCHEMY_KEY", "xyz")
    out = normalize_url("https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}/")
    assert out == "https://eth-mainnet.g.alchemy.com/v2/xyz"


def test_normalize_url_expands_multiple_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOST", "example.com")
    monkeypatch.setenv("PATH", "v3/sekret")
    out = normalize_url("https://$HOST/$PATH")
    assert out == "https://example.com/v3/sekret"


def test_normalize_url_raises_on_missing_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_KEY", raising=False)
    with pytest.raises(ValueError, match="MISSING_KEY.*not set|not.*set"):
        normalize_url("https://example.com/$MISSING_KEY")


def test_normalize_url_lowercases_scheme_and_host_and_strips_root_slash() -> None:
    assert normalize_url("HTTPS://EXAMPLE.COM/") == "https://example.com"


def test_normalize_url_collapses_default_ports() -> None:
    assert normalize_url("https://example.com:443") == "https://example.com"
    assert normalize_url("http://example.com:80/") == "http://example.com"
    assert normalize_url("wss://example.com:443") == "wss://example.com"
    assert normalize_url("ws://example.com:80/") == "ws://example.com"


def test_normalize_url_preserves_non_default_ports() -> None:
    assert normalize_url("https://example.com:8443") == "https://example.com:8443"


def test_is_url_target_is_prefix_based_and_does_not_match_ipc() -> None:
    assert is_url_target("https://example.com") is True
    assert is_url_target("wss://example.com") is True
    assert is_url_target("ipc:///tmp/geth.ipc") is False
    assert is_url_target(" /tmp/geth.ipc ") is False


def test_normalize_target_normalizes_urls_but_preserves_non_urls() -> None:
    assert normalize_target("  HTTP://EXAMPLE.COM/ ") == "http://example.com"
    assert normalize_target(" ipc:///tmp/geth.ipc ") == "ipc:///tmp/geth.ipc"
