# src/fastweb3/web3/web3.py
"""High-level Web3 client.

Most users should interact with `Web3` (and its namespaces such as
`Web3.eth`).
"""

from __future__ import annotations

import threading
import weakref
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from lazy_object_proxy import Proxy

from ..deferred import Handle, deferred_response
from ..endpoint import Endpoint
from ..env import (
    get_default_primary_endpoint,
    get_pool_mode,
    resolve_primary_endpoint,
    should_use_pool,
)
from ..errors import NoEndpoints, RPCError
from ..formatters import to_int
from ..provider import Provider, RetryPolicy
from ..provider.pool import acquire_pool_manager, release_pool_manager
from .batch import _NEVER_BATCH_METHODS, FreshnessFn, _BatchManager, _Queued, _tls_state
from .eth import Eth

_DEFAULT_PRIMARY_CHAIN_ID_LOCK = threading.Lock()
_DEFAULT_PRIMARY_CHAIN_ID_SET = False
_DEFAULT_PRIMARY_CHAIN_ID: Optional[int] = None


@dataclass(frozen=True)
class Web3Config:
    """Configuration for `Web3`.

    Attributes:
        strict: If ``True``, validate and normalize inputs for common RPC
            methods.
        desired_pool_size: Desired total pool size for pool routing.
        retry_policy_pool: Retry policy used when routing via the pool.
    """

    strict: bool = True
    desired_pool_size: int = 6
    retry_policy_pool: RetryPolicy = RetryPolicy(max_attempts=3, backoff_seconds=0.05)


def _get_default_primary_chain_id_once() -> Optional[int]:
    """Probe the chain ID of ``FASTWEB3_PRIMARY_ENDPOINT`` once per process.

    Returns:
        The discovered chain ID, or ``None`` if the env var is unset or the
        probe fails.
    """
    global _DEFAULT_PRIMARY_CHAIN_ID_SET, _DEFAULT_PRIMARY_CHAIN_ID

    if _DEFAULT_PRIMARY_CHAIN_ID_SET:
        return _DEFAULT_PRIMARY_CHAIN_ID

    with _DEFAULT_PRIMARY_CHAIN_ID_LOCK:
        if _DEFAULT_PRIMARY_CHAIN_ID_SET:
            return _DEFAULT_PRIMARY_CHAIN_ID

        target = get_default_primary_endpoint()
        if not target:
            _DEFAULT_PRIMARY_CHAIN_ID = None
            _DEFAULT_PRIMARY_CHAIN_ID_SET = True
            return None

        ep = Endpoint(target)
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
        target_pool: int = 6,
        max_lag_blocks: int = 8,
        probe_timeout_s: float = 1.5,
        probe_workers: int = 32,
    ) -> None:
        """Create a `Web3` client.

        Args:
            chain_id: Optional chain ID used for pool discovery.
            endpoints: Optional list of explicit endpoints to use as the
                internal pool.
            primary_endpoint: Optional primary endpoint target.
            provider: Optional fully custom provider (advanced).
            config: Optional `Web3Config`.
            target_pool: Target pool size for public discovery.
            max_lag_blocks: Maximum tolerated lag behind the best observed tip.
            probe_timeout_s: Per-probe timeout for pool discovery.
            probe_workers: Maximum number of probe worker threads.

        Raises:
            NoEndpoints: If no usable configuration is provided.
        """
        self.config = config or Web3Config()
        self._chain_id = int(chain_id) if chain_id is not None else None
        self._pool_chain_id: Optional[int] = None
        self._pool_finalizer: weakref.finalize | None = None

        if provider is not None:
            self.provider = provider
        else:
            internal_endpoints = list(endpoints or [])

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
                    pool_manager = acquire_pool_manager(
                        int(chain_id),
                        target_pool=target_pool,
                        max_lag_blocks=max_lag_blocks,
                        probe_timeout_s=probe_timeout_s,
                        probe_workers=probe_workers,
                    )
            if pool_manager is not None and chain_id is not None:
                self._pool_chain_id = int(chain_id)
                self._pool_finalizer = weakref.finalize(self, release_pool_manager, int(chain_id))

            # Allow env default primary-only mode when no chain_id/endpoints/primary provided
            env_default_primary = None
            if primary_endpoint is None:
                env_default_primary = get_default_primary_endpoint()

            if (
                chain_id is None
                and not internal_endpoints
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
                internal_endpoints,
                pool_manager=pool_manager,
                desired_pool_size=self.config.desired_pool_size,
                retry_policy_pool=self.config.retry_policy_pool,
            )

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

        if primary_endpoint is not None:
            self.provider.set_primary(primary_endpoint)

        self.eth = Eth(self)

    def close(self) -> None:
        """Close resources held by this client."""
        fin = self._pool_finalizer
        if fin is not None and fin.alive:
            fin()
        self.provider.close()

    def _active_methods_filter(self) -> set[str] | None:
        st = _tls_state()
        if st.methods_stack is None or not st.methods_stack:
            return None
        return st.methods_stack[-1]

    def _should_batch(self, method: str) -> bool:
        if method in _NEVER_BATCH_METHODS:
            return False
        filt = self._active_methods_filter()
        if filt is None:
            return True
        return method in filt

    def _enqueue_batch(
        self,
        method: str,
        params: list[Any],
        *,
        route: str,
        formatter,
        freshness: FreshnessFn | None,
        handle: Handle,
    ) -> None:
        st = _tls_state()
        assert st.queue is not None
        st.queue.append(
            _Queued(
                method=method,
                params=params,
                route=route,
                formatter=formatter,
                freshness=freshness,
                handle=handle,
            )
        )

    def _flush_batch(self, *, raise_on_error: bool) -> None:
        st = _tls_state()
        if st.flushing:
            return
        if st.queue is None or not st.queue:
            return

        st.flushing = True
        try:
            queue = list(st.queue)
            st.queue.clear()

            i = 0
            first_rpc_err: RPCError | None = None

            # Preserve order while allowing mixed routes by flushing contiguous segments.
            while i < len(queue):
                route = queue[i].route
                j = i
                while j < len(queue) and queue[j].route == route:
                    j += 1
                chunk = queue[i:j]

                calls = [(q.method, q.params, q.formatter, q.freshness) for q in chunk]
                out = self.provider.request_batch(calls, route=route)

                for q, item in zip(chunk, out):
                    if isinstance(item, RPCError):
                        if first_rpc_err is None:
                            first_rpc_err = item
                        q.handle.set_exc(item)
                    else:
                        q.handle.set_value(item)

                i = j

            if first_rpc_err is not None and raise_on_error:
                raise first_rpc_err

        finally:
            st.flushing = False

    def make_request(
        self,
        method: str,
        params: list[Any],
        *,
        route: str = "pool",
        formatter=None,
        freshness: FreshnessFn | None = None,
    ) -> Any:
        """Perform a raw JSON-RPC request.

        This is the lowest-level request API on `Web3`.

        Args:
            method: JSON-RPC method name (e.g. ``"eth_getBalance"``).
            params: JSON-RPC params list. No validation is performed on this
                list.
            route: Routing hint: ``"pool"`` or ``"primary"``.
            formatter: Optional post-processor for the result value.
            freshness: Optional freshness predicate
                ``freshness(response, required_tip, returned_tip) -> bool``.

        Returns:
            The (optionally formatted) result.

        Raises:
            RPCError: If the response includes a JSON-RPC error object.
            TransportError: For transport-level failures.
        """
        st = _tls_state()
        if st.depth > 0 and not st.flushing and self._should_batch(method):

            def ref_func(_: Handle) -> None:
                self._flush_batch(raise_on_error=True)

            h = Handle(bg_func=None, format_func=formatter, ref_func=ref_func)
            self._enqueue_batch(
                method,
                params,
                route=route,
                formatter=None,
                freshness=freshness,
                handle=h,
            )
            return Proxy(h.get_value)

        def bg_func(h: Handle) -> None:
            raw = self.provider.request(
                method,
                params,
                route=route,
                formatter=None,
                freshness=freshness,
            )
            h.set_value(raw)

        return deferred_response(bg_func, format_func=formatter, ref_func=None)

    def batch_requests(self, methods: set[str] | None = None) -> _BatchManager:
        """Context manager that batches eligible requests issued inside the scope.

        Args:
            methods: Optional set of method names to batch. If omitted, all
                methods are eligible except those in the internal
                ``_NEVER_BATCH_METHODS`` set.

        Returns:
            A context manager. Requests issued inside the ``with`` scope return
            proxy objects that resolve when the batch flushes.
        """
        return _BatchManager(self, methods)
