import pytest

from fastweb3.utils import normalize_url


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
