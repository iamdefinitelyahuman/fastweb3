from urllib.parse import urlsplit, urlunsplit


def normalize_url(url: str) -> str:
    """
    Normalize URLs for deduplication.

    - strip whitespace
    - lowercase scheme + host
    - collapse default port
    - normalize path (strip trailing slash except root)
    - preserve query/fragment
    """
    url = url.strip()
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
