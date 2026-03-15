# fastweb3/env.py
"""Environment-variable configuration helpers.

These helpers read environment variables that influence endpoint selection:

* ``FASTWEB3_USE_PUBLIC_POOL`` controls whether the public pool is used.
* ``FASTWEB3_PRIMARY_ENDPOINT`` and ``FASTWEB3_PRIMARY_ENDPOINTS`` configure
  primary (node-local) endpoints.
"""

from __future__ import annotations

import os
from typing import Dict, Optional

_ENV_PRIMARY = "FASTWEB3_PRIMARY_ENDPOINT"
_ENV_PRIMARY_MAP = "FASTWEB3_PRIMARY_ENDPOINTS"
_ENV_USE_PUBLIC_POOL = "FASTWEB3_USE_PUBLIC_POOL"


def get_use_public_pool(env: Optional[dict[str, str]] = None) -> Optional[bool]:
    """Return the configured public-pool preference, if any.

    ``FASTWEB3_USE_PUBLIC_POOL`` values:

    * unset: no environment preference.
    * ``true`` / ``1`` / ``yes`` / ``on``: prefer using the public pool.
    * ``false`` / ``0`` / ``no`` / ``off``: prefer not using the public pool,
      but only for chains that have a configured primary endpoint.

    Args:
        env: Optional environment mapping to read from. Defaults to
            :data:`os.environ`.

    Returns:
        ``True``, ``False``, or ``None`` if unset.

    Raises:
        ValueError: If ``FASTWEB3_USE_PUBLIC_POOL`` has an unrecognized value.
    """
    env = os.environ if env is None else env
    raw = env.get(_ENV_USE_PUBLIC_POOL)
    if raw is None:
        return None

    raw = raw.strip().lower()
    if raw == "":
        return None
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False

    raise ValueError(
        f"{_ENV_USE_PUBLIC_POOL} must be a boolean: true/false, 1/0, yes/no, on/off (got {raw!r})"
    )


def get_default_primary_endpoint(env: Optional[dict[str, str]] = None) -> Optional[str]:
    """Return the global primary endpoint, if configured.

    This reads ``FASTWEB3_PRIMARY_ENDPOINT``.

    Args:
        env: Optional environment mapping to read from. Defaults to
            :data:`os.environ`.

    Returns:
        The configured endpoint URL/target, or ``None`` if unset/empty.
    """
    env = os.environ if env is None else env
    val = env.get(_ENV_PRIMARY)
    if val is None:
        return None
    val = val.strip()
    return val or None


def parse_primary_endpoints(env: Optional[dict[str, str]] = None) -> Dict[int, str]:
    """Parse per-chain primary endpoints from ``FASTWEB3_PRIMARY_ENDPOINTS``.

    The expected format is a semicolon-separated list:
    ``"1=https://...;10=https://...;8453=https://..."``.

    Args:
        env: Optional environment mapping to read from. Defaults to
            :data:`os.environ`.

    Returns:
        Mapping of ``chain_id`` to endpoint URL.

    Raises:
        ValueError: If an entry is malformed.
    """
    env = os.environ if env is None else env
    raw = (env.get(_ENV_PRIMARY_MAP) or "").strip()
    if not raw:
        return {}

    out: Dict[int, str] = {}
    parts = raw.split(";")
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # split only on the first '=' so URLs can contain '=' safely
        k, sep, v = part.partition("=")
        if sep != "=":
            raise ValueError(
                f"{_ENV_PRIMARY_MAP} entry must be 'chainid=url' (bad entry: {part!r})"
            )

        k = k.strip()
        v = v.strip()
        if not k or not v:
            raise ValueError(
                f"{_ENV_PRIMARY_MAP} entry must be 'chainid=url' (bad entry: {part!r})"
            )

        try:
            chain_id = int(k, 10)
        except ValueError as e:
            raise ValueError(
                f"{_ENV_PRIMARY_MAP} chainid must be an int (bad chainid: {k!r})"
            ) from e

        out[chain_id] = v

    return out


def resolve_primary_endpoint(
    chain_id: int,
    *,
    env: Optional[dict[str, str]] = None,
    default_primary_chain_id: Optional[int] = None,
) -> Optional[str]:
    """Resolve the primary endpoint (if any) for a given chain.

    Resolution order:

    1. ``FASTWEB3_PRIMARY_ENDPOINTS[chain_id]`` (per-chain) wins.
    2. Otherwise, ``FASTWEB3_PRIMARY_ENDPOINT`` (global) may apply.

    A global primary only applies to the chain that the global endpoint is on.
    To enforce that, pass ``default_primary_chain_id`` (e.g. discovered by
    calling ``eth_chainId`` on the global endpoint).

    Args:
        chain_id: Chain ID to resolve.
        env: Optional environment mapping to read from. Defaults to
            :data:`os.environ`.
        default_primary_chain_id: Chain ID of the global primary endpoint, if
            known.

    Returns:
        Primary endpoint URL/target, or ``None`` if no primary applies.
    """
    env = os.environ if env is None else env

    per_chain = parse_primary_endpoints(env)
    if chain_id in per_chain:
        return per_chain[chain_id]

    default_primary = get_default_primary_endpoint(env)
    if default_primary is None:
        return None

    if default_primary_chain_id is not None and chain_id != default_primary_chain_id:
        return None

    return default_primary


def should_use_pool(
    chain_id: int,
    *,
    env: Optional[dict[str, str]] = None,
    default_primary_chain_id: Optional[int] = None,
) -> bool:
    """Return whether the public pool should be used for a chain.

    Args:
        chain_id: Chain ID to check.
        env: Optional environment mapping to read from. Defaults to
            :data:`os.environ`.
        default_primary_chain_id: Chain ID of the global primary endpoint, if
            known.

    Returns:
        ``True`` if the pool should be used, otherwise ``False``.
    """
    env = os.environ if env is None else env
    use_public_pool = get_use_public_pool(env)

    if use_public_pool is None or use_public_pool is True:
        return True

    primary = resolve_primary_endpoint(
        chain_id, env=env, default_primary_chain_id=default_primary_chain_id
    )
    return primary is None
