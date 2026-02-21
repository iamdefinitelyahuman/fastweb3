from urllib.parse import urlsplit, urlunsplit


def normalize_url(url: str) -> str:
    """
    Normalize URLs for deduplication.

    - strip whitespace
    - lowercase scheme + host
    - strip trailing slash from path
    - preserve query/fragment (some providers may use path/query routing)
    """
    url = url.strip()
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, parts.query, parts.fragment))
