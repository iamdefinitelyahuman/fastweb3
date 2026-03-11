"""Batch request machinery for :mod:`fastweb3.web3`.

This module contains the thread-local state and context managers used by
``Web3.batch_requests()``. Most users should interact with batching through the
high-level ``Web3`` API rather than importing these helpers directly.
"""

from __future__ import annotations

import threading
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from ..deferred import Handle

if TYPE_CHECKING:
    from .web3 import Web3


FreshnessFn = Callable[[Any, int, int], bool]


_NEVER_BATCH_METHODS: set[str] = {
    # side-effecting / timing-sensitive
    "eth_sendRawTransaction",
    "eth_sendTransaction",
    # filter lifecycle (server-side state)
    "eth_newFilter",
    "eth_newBlockFilter",
    "eth_newPendingTransactionFilter",
    "eth_uninstallFilter",
    "eth_getFilterChanges",
    "eth_getFilterLogs",
    # signing (often wallet-backed)
    "eth_sign",
    "eth_signTransaction",
    "eth_signTypedData",
    "eth_signTypedData_v3",
    "eth_signTypedData_v4",
    "personal_sign",
}


@dataclass
class _Queued:
    """A single request queued for a pending batch flush.

    Attributes:
        method: JSON-RPC method name.
        params: JSON-RPC params to send.
        route: Routing hint used for the request.
        formatter: Optional formatter to apply after resolution.
        freshness: Optional freshness predicate for route-level freshness
            validation.
        handle: Deferred handle used to resolve the queued request.
    """

    method: str
    params: list[Any]
    route: str
    formatter: Any
    freshness: FreshnessFn | None
    handle: Handle


@dataclass
class _BatchState:
    """Thread-local mutable state for active batch scopes.

    Attributes:
        depth: Nesting depth of active batch contexts in the current thread.
        flushing: Whether a batch flush is currently in progress.
        queue: Requests waiting to be flushed.
        methods_stack: Stack of active method filters for nested scopes.
        batch_id: Identifier for the current top-level batch scope.
    """

    depth: int = 0
    flushing: bool = False
    queue: list[_Queued] | None = None
    methods_stack: list[set[str] | None] | None = None
    batch_id: int = 0

    def __post_init__(self) -> None:
        """Initialize mutable containers lazily for dataclass defaults."""
        if self.queue is None:
            self.queue = []
        if self.methods_stack is None:
            self.methods_stack = []


_TLS = threading.local()
_BATCH_ID_LOCK = threading.Lock()
_BATCH_ID_NEXT = 1


def _next_batch_id() -> int:
    """Return the next process-local batch identifier."""
    global _BATCH_ID_NEXT
    with _BATCH_ID_LOCK:
        bid = _BATCH_ID_NEXT
        _BATCH_ID_NEXT += 1
        return bid


def _tls_state() -> _BatchState:
    """Return the current thread's batch state, creating it if needed."""
    st = getattr(_TLS, "batch_state", None)
    if st is None:
        st = _BatchState()
        setattr(_TLS, "batch_state", st)
    return st


class _BatchContext:
    """Introspection helpers for an active batch scope.

    Instances of this class are returned from ``Web3.batch_requests()``.
    """

    def __init__(self, w3: "Web3", batch_id: int) -> None:
        """Create a batch context bound to a specific ``Web3`` batch scope.

        Args:
            w3: ``Web3`` instance that owns the active batch scope.
            batch_id: Identifier of the active top-level batch scope.
        """
        self._w3 = w3
        self._batch_id = batch_id

    def _state(self) -> _BatchState:
        """Return the current thread-local state for this batch context.

        Raises:
            RuntimeError: If the context is no longer active in the current
                thread.
        """
        st = _tls_state()
        if st.depth <= 0 or st.batch_id != self._batch_id:
            raise RuntimeError("Batch context is no longer active in this thread")
        return st

    def flush(self) -> None:
        """Flush all currently queued calls.

        Raises:
            RPCError: If any queued call resulted in a JSON-RPC error.
        """
        self._w3._flush_batch(raise_on_error=True)

    def pending_count(self) -> int:
        """Return the number of queued calls that have not been flushed."""
        return len(self._state().queue or [])

    def pending_methods(self) -> dict[str, int]:
        """Return a count of queued method names."""
        c: Counter[str] = Counter()
        for q in self._state().queue or []:
            c[q.method] += 1
        return dict(c)

    def pending_preview(self, n: int = 10) -> list[dict[str, Any]]:
        """Return a lightweight preview of the first ``n`` queued calls.

        Args:
            n: Maximum number of queued items to include.

        Returns:
            A list of preview dictionaries containing the method name, route,
            and parameter count for each queued item.
        """
        out: list[dict[str, Any]] = []
        for q in (self._state().queue or [])[: max(0, int(n))]:
            out.append(
                {
                    "method": q.method,
                    "route": q.route,
                    "params_len": len(q.params),
                }
            )
        return out

    def describe(self) -> str:
        """Return a human-friendly description of the current batch state."""
        st = self._state()
        return (
            f"BatchContext(depth={st.depth}, pending={len(st.queue or [])}, "
            f"flushing={st.flushing}, methods={self.pending_methods()})"
        )


class _BatchManager:
    """Context manager that owns a batch scope for a ``Web3`` instance."""

    def __init__(self, w3: "Web3", methods: set[str] | None) -> None:
        """Create a batch manager.

        Args:
            w3: ``Web3`` instance whose requests will be batched.
            methods: Optional set of method names eligible for batching within
                the scope. ``None`` means batch all eligible methods.
        """
        self._w3 = w3
        self._methods = methods

    def __enter__(self) -> _BatchContext:
        """Enter a batch scope and return its introspection context."""
        st = _tls_state()

        # Nesting boundary: flush current queue before entering deeper scope.
        if st.depth > 0 and (st.queue or []):
            self._w3._flush_batch(raise_on_error=True)

        st.depth += 1
        if st.depth == 1:
            st.batch_id = _next_batch_id()

        # Push active methods filter (None => batch all eligible methods)
        st.methods_stack.append(self._methods)

        return _BatchContext(self._w3, st.batch_id)

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Flush queued work as the scope exits.

        Args:
            exc_type: Exception type raised inside the ``with`` block, if any.
            exc: Exception instance raised inside the ``with`` block, if any.
            tb: Traceback for the exception, if any.

        Returns:
            ``False`` so any exception raised inside the scope propagates.
        """
        st = _tls_state()
        raise_on_error = exc_type is None

        try:
            # Nesting boundary: flush on leaving the scope.
            if st.depth > 0 and (st.queue or []):
                self._w3._flush_batch(raise_on_error=raise_on_error)
        finally:
            if st.methods_stack:
                st.methods_stack.pop()

            st.depth = max(0, st.depth - 1)
            if st.depth == 0:
                if st.queue is not None:
                    st.queue.clear()
                if st.methods_stack is not None:
                    st.methods_stack.clear()
                st.flushing = False
                st.batch_id = 0

        return False
