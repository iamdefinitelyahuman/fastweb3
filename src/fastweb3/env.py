# fastweb3/env.py
from __future__ import annotations

import os
from typing import Dict, Literal, Optional

PoolMode = Literal["default", "split", "off"]

_ENV_PRIMARY = "FASTWEB3_PRIMARY_ENDPOINT"
_ENV_PRIMARY_MAP = "FASTWEB3_PRIMARY_ENDPOINTS"
_ENV_POOL_MODE = "FASTWEB3_POOL_MODE"


def get_pool_mode(env: Optional[dict[str, str]] = None) -> PoolMode:
    """
    FASTWEB3_POOL_MODE:
      - unset / "default": use pool everywhere
      - "split": disable pool only on chains that have a configured primary
                (for FASTWEB3_PRIMARY_ENDPOINT this requires knowing its chain id)
      - "off": never use pool
    """
    env = os.environ if env is None else env
    raw = env.get(_ENV_POOL_MODE, "").strip().lower()

    if raw in ("", "default"):
        return "default"
    if raw in ("split", "off"):
        return raw  # type: ignore[return-value]

    raise ValueError(f"{_ENV_POOL_MODE} must be one of: default, split, off (got {raw!r})")


def get_default_primary_endpoint(env: Optional[dict[str, str]] = None) -> Optional[str]:
    env = os.environ if env is None else env
    val = env.get(_ENV_PRIMARY)
    if val is None:
        return None
    val = val.strip()
    return val or None


def parse_primary_endpoints(env: Optional[dict[str, str]] = None) -> Dict[int, str]:
    """
    FASTWEB3_PRIMARY_ENDPOINTS format:
        "1=https://...;10=https://...;8453=https://..."
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
    """
    Returns the primary endpoint URL (if any) for `chain_id`, based on env vars.

    Resolution:
      1) FASTWEB3_PRIMARY_ENDPOINTS[chain_id] (per-chain) wins.
      2) else FASTWEB3_PRIMARY_ENDPOINT (global) may apply.

    Important behavior for POOL_MODE=split:
      - A per-chain primary always counts as "configured primary" for that chain.
      - A global primary only counts for the chain that *the global endpoint is on*.
        To enforce that, pass `default_primary_chain_id` (discovered elsewhere,
        e.g. by calling eth_chainId on the global endpoint directly).
    """
    env = os.environ if env is None else env

    per_chain = parse_primary_endpoints(env)
    if chain_id in per_chain:
        return per_chain[chain_id]

    default_primary = get_default_primary_endpoint(env)
    if default_primary is None:
        return None

    mode = get_pool_mode(env)
    if mode == "split" and default_primary_chain_id is not None:
        # Only treat the global primary as applicable to its own chain.
        if chain_id != default_primary_chain_id:
            return None

    return default_primary


def should_use_pool(
    chain_id: int,
    *,
    env: Optional[dict[str, str]] = None,
    default_primary_chain_id: Optional[int] = None,
) -> bool:
    """
    Returns whether the pool manager should be used for `chain_id` under env config.
    """
    env = os.environ if env is None else env
    mode = get_pool_mode(env)

    if mode == "off":
        return False
    if mode == "default":
        return True

    # split: pool only when there is no configured primary for this chain
    primary = resolve_primary_endpoint(
        chain_id, env=env, default_primary_chain_id=default_primary_chain_id
    )
    return primary is None
