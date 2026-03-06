# src/fastweb3/utils.py
"""Small helpers for target normalization and environment expansion."""

import os
import re
from urllib.parse import urlsplit, urlunsplit

_ENV_VAR_RE = re.compile(r"\$(\w+)|\$\{([^}]+)\}")


def _expand_env_vars(s: str) -> str:
    """Expand ``$VARNAME`` and ``${VARNAME}`` using :data:`os.environ`.

    Args:
        s: String containing environment variable references.

    Returns:
        The expanded string.

    Raises:
        ValueError: If a referenced environment variable is not set.
    """

    def repl(m: re.Match[str]) -> str:
        name = m.group(1) or m.group(2)  # $FOO or ${FOO}
        assert name is not None
        val = os.environ.get(name)
        if val is None:
            raise ValueError(f"Environment variable '{name}' is not set")
        return val

    return _ENV_VAR_RE.sub(repl, s)


def is_url_target(target: str) -> bool:
    """Return ``True`` if ``target`` is an HTTP(S)/WS(S) URL.

    This is deliberately a simple prefix check to avoid mis-parsing IPC paths.

    Args:
        target: Endpoint target string.

    Returns:
        ``True`` for HTTP(S)/WS(S) URLs, otherwise ``False``.
    """
    t = target.strip().lower()
    return t.startswith(("http://", "https://", "ws://", "wss://"))


def normalize_target(target: str) -> str:
    """Normalize an endpoint target for deduplication.

    The normalization steps are:

    * expand environment variables (``$FOO`` / ``${FOO}``)
    * strip whitespace
    * if the target is a URL, apply `normalize_url()`
    * otherwise return the expanded/stripped value unchanged (IPC paths, etc.)

    Args:
        target: Endpoint target string.

    Returns:
        Normalized target.
    """
    t = _expand_env_vars(target).strip()
    if is_url_target(t):
        return normalize_url(t)
    return t


def normalize_url(url: str) -> str:
    """Normalize a URL for deduplication.

    URL normalization includes:

    * expand environment variables (``$FOO`` / ``${FOO}``)
    * strip whitespace
    * lowercase scheme + host
    * collapse default ports
    * normalize the path (strip trailing slash except root)
    * preserve query/fragment

    Args:
        url: URL string.

    Returns:
        Normalized URL string.
    """
    url = _expand_env_vars(url).strip()
    parts = urlsplit(url)

    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    port = parts.port

    # Remove default ports
    if (
        (scheme == "http" and port == 80)
        or (scheme == "https" and port == 443)
        or (scheme == "ws" and port == 80)
        or (scheme == "wss" and port == 443)
    ):
        netloc = hostname
    else:
        netloc = hostname if port is None else f"{hostname}:{port}"

    path = parts.path or ""
    if path != "/":
        path = path.rstrip("/")
    else:
        path = ""

    return urlunsplit((scheme, netloc, path, parts.query, parts.fragment))
