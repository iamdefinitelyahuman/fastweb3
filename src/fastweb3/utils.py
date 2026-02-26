import os
import re
from urllib.parse import urlsplit, urlunsplit

_ENV_VAR_RE = re.compile(r"\$(\w+)|\$\{([^}]+)\}")


def _expand_env_vars(s: str) -> str:
    """
    Expand $VARNAME and ${VARNAME} using os.environ.

    Raises ValueError if a referenced env var is not set.
    """

    def repl(m: re.Match[str]) -> str:
        name = m.group(1) or m.group(2)  # $FOO or ${FOO}
        assert name is not None
        val = os.environ.get(name)
        if val is None:
            raise ValueError(f"Environment variable '{name}' is not set")
        return val

    return _ENV_VAR_RE.sub(repl, s)


def normalize_url(url: str) -> str:
    """
    Normalize URLs for deduplication.

    - expand env vars ($FOO / ${FOO})
    - strip whitespace
    - lowercase scheme + host
    - collapse default port
    - normalize path (strip trailing slash except root)
    - preserve query/fragment
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
