# src/fastweb3/web3/web3.py
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from ..deferred import Handle, deferred_response
from ..endpoint import Endpoint
from ..env import (
    get_default_primary_endpoint,
    get_pool_mode,
    resolve_primary_endpoint,
    should_use_pool,
)
from ..errors import NoEndpoints
from ..formatters import to_int
from ..provider import Provider, RetryPolicy
from ..rpc_pool import get_pool_manager
from .eth import Eth

_DEFAULT_PRIMARY_CHAIN_ID_LOCK = threading.Lock()
_DEFAULT_PRIMARY_CHAIN_ID_SET = False
_DEFAULT_PRIMARY_CHAIN_ID: Optional[int] = None


FreshnessFn = Callable[[Any, int, int], bool]


@dataclass(frozen=True)
class Web3Config:
    strict: bool = True
    desired_pool_size: int = 6
    retry_policy_pool: RetryPolicy = RetryPolicy(max_attempts=3, backoff_seconds=0.05)
    # later: batching/hedging/quorum knobs
    # later: output formatting mode knobs (raw vs normalized)


def _get_default_primary_chain_id_once() -> Optional[int]:
    """
    Probe FASTWEB3_PRIMARY_ENDPOINT's chain id once per process.
    Returns None if not set or probe fails.
    """
    global _DEFAULT_PRIMARY_CHAIN_ID_SET, _DEFAULT_PRIMARY_CHAIN_ID

    if _DEFAULT_PRIMARY_CHAIN_ID_SET:
        return _DEFAULT_PRIMARY_CHAIN_ID

    with _DEFAULT_PRIMARY_CHAIN_ID_LOCK:
        if _DEFAULT_PRIMARY_CHAIN_ID_SET:
            return _DEFAULT_PRIMARY_CHAIN_ID

        url = get_default_primary_endpoint()
        if not url:
            _DEFAULT_PRIMARY_CHAIN_ID = None
            _DEFAULT_PRIMARY_CHAIN_ID_SET = True
            return None

        ep = Endpoint(url)
        try:
            _DEFAULT_PRIMARY_CHAIN_ID = ep.request("eth_chainId", (), formatter=to_int)
        except Exception:
            _DEFAULT_PRIMARY_CHAIN_ID = None
        finally:
            ep.close()

        _DEFAULT_PRIMARY_CHAIN_ID_SET = True
        return _DEFAULT_PRIMARY_CHAIN_ID


class Web3:
    """
    Web3 entrypoint.

    Usage:
        w3 = Web3(1)                                       # public pool for chain 1 (lazy-ready)
        w3 = Web3(endpoints=[...])                         # manual internal pool only
        w3 = Web3(1, endpoints=[...])                      # hybrid: internal pool + public pool
        w3 = Web3(primary_endpoint="http://localhost:8545")# primary-only mode
        w3 = Web3(1, primary_endpoint="http://localhost")  # hybrid: public pool + explicit primary
        w3 = Web3(provider=my_provider)                    # fully custom provider (advanced)
    """

    def __init__(
        self,
        chain_id: Optional[int] = None,
        *,
        endpoints: Optional[Sequence[str]] = None,
        primary_endpoint: Optional[str] = None,
        provider: Optional[Provider] = None,
        config: Optional[Web3Config] = None,
        # pool manager tuning (only used when chain_id is provided)
        target_pool: int = 6,
        max_lag_blocks: int = 8,
        probe_timeout_s: float = 1.5,
        probe_workers: int = 32,
    ) -> None:
        self.config = config or Web3Config()
        self._chain_id = int(chain_id) if chain_id is not None else None

        if provider is not None:
            self.provider = provider

        else:
            internal_urls = list(endpoints or [])

            # If we're in env split mode, we need to know which chain the *global*
            # FASTWEB3_PRIMARY_ENDPOINT is on, so we can disable pool only there.
            env_default_primary_chain_id: Optional[int] = None
            if (
                primary_endpoint is None
                and chain_id is not None
                and get_pool_mode() == "split"
                and get_default_primary_endpoint() is not None
            ):
                env_default_primary_chain_id = _get_default_primary_chain_id_once()

            pool_manager = None
            if chain_id is not None:
                if should_use_pool(
                    int(chain_id),
                    default_primary_chain_id=env_default_primary_chain_id,
                ):
                    pool_manager = get_pool_manager(
                        int(chain_id),
                        target_pool=target_pool,
                        max_lag_blocks=max_lag_blocks,
                        probe_timeout_s=probe_timeout_s,
                        probe_workers=probe_workers,
                    )

            # Allow env default primary-only mode when no chain_id/endpoints/primary provided
            env_default_primary = None
            if primary_endpoint is None:
                env_default_primary = get_default_primary_endpoint()

            if (
                chain_id is None
                and not internal_urls
                and primary_endpoint is None
                and env_default_primary is None
            ):
                extra = ""
                if get_pool_mode() == "off":
                    extra = " (pool disabled by FASTWEB3_POOL_MODE=off)"
                raise NoEndpoints(
                    "No chain_id provided and no endpoints provided. "
                    "Use Web3(<chain_id>) for public discovery, "
                    "Web3(endpoints=[...]) for manual mode, "
                    "or Web3(primary_endpoint=...) for primary-only mode." + extra
                )

            self.provider = Provider(
                internal_urls,
                pool_manager=pool_manager,
                desired_pool_size=self.config.desired_pool_size,
                retry_policy_pool=self.config.retry_policy_pool,
            )

            # Apply env primary if caller didn't pass one explicitly
            if primary_endpoint is None:
                if chain_id is not None:
                    env_primary_for_chain = resolve_primary_endpoint(
                        int(chain_id),
                        default_primary_chain_id=env_default_primary_chain_id,
                    )
                    if env_primary_for_chain is not None:
                        self.provider.set_primary(env_primary_for_chain)
                elif env_default_primary is not None:
                    self.provider.set_primary(env_default_primary)

        # Explicit primary always wins (over provider/env)
        if primary_endpoint is not None:
            self.provider.set_primary(primary_endpoint)

        # Namespaces
        self.eth = Eth(self)

    def close(self) -> None:
        self.provider.close()

    def make_request(
        self,
        method: str,
        params: list[Any],
        *,
        route: str = "pool",
        formatter=None,
        freshness: FreshnessFn | None = None,
    ) -> Any:
        """
        Perform a raw JSON-RPC request.

        method: e.g. "eth_getBalance"
        params: JSON-RPC params list. No validation is performed on this list.
        route: "pool" or "primary" (routing hint)
        formatter: optional post-processor for result
        freshness: optional freshness policy:
            freshness(response, required_tip, returned_tip) -> bool
        """

        def bg_func(h: Handle) -> None:
            raw = self.provider.request(
                method,
                params,
                route=route,
                formatter=None,
                freshness=freshness,
            )
            h.set_value(raw)

        # ref_func unused for now (later: batching flush barrier)
        return deferred_response(bg_func, format_func=formatter, ref_func=None)

    # ---- scaffolding for later batching ----

    def pin(self, *, route: str = "pool"):
        return self.provider.pin(route=route)

    def batch(self):
        raise NotImplementedError("Batching not implemented yet")
