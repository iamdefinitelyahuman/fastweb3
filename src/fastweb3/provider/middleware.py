"""Provider middleware orchestration.

This mixin implements the execution order described by
`fastweb3.middleware.ProviderMiddleware`.
"""

from __future__ import annotations

from ..errors import RPCError
from ..middleware import MiddlewareContext
from .types import _BatchCall


class MiddlewareMixin:
    """Mixin implementing middleware registration and hook execution."""

    def add_middleware(self, mw, *, prepend: bool = False) -> None:
        """Attach a middleware to this provider.

        Args:
            mw: Middleware instance.
            prepend: If ``True``, place the middleware at the front of the
                chain.
        """
        with self._lock:
            if prepend:
                self._middlewares.insert(0, mw)
            else:
                self._middlewares.append(mw)

    def _run_middlewares_before(
        self, ctx: MiddlewareContext, calls: list[_BatchCall]
    ) -> list[_BatchCall]:
        """Run ``before_request`` hooks in attach order."""
        with self._lock:
            mws = list(self._middlewares)
        for mw in mws:
            fn = getattr(mw, "before_request", None)
            if fn is not None:
                calls = fn(ctx, calls)
        return calls

    def _run_middlewares_after(
        self,
        ctx: MiddlewareContext,
        calls: list[_BatchCall],
        results: list[object | RPCError],
    ) -> list[object | RPCError]:
        """Run ``after_request`` hooks in reverse attach order."""
        with self._lock:
            mws = list(self._middlewares)
        for mw in reversed(mws):
            fn = getattr(mw, "after_request", None)
            if fn is not None:
                results = fn(ctx, calls, results)
        return results

    def _run_middlewares_on_exception(
        self,
        ctx: MiddlewareContext,
        calls: list[_BatchCall],
        exc: Exception,
    ) -> list[object | RPCError] | Exception:
        """Run ``on_exception`` hooks in reverse attach order.

        The chain stops early once a middleware returns a non-exception
        (recovered results).
        """
        with self._lock:
            mws = list(self._middlewares)
        out: list[object | RPCError] | Exception = exc
        for mw in reversed(mws):
            fn = getattr(mw, "on_exception", None)
            if fn is None:
                continue
            if not isinstance(out, Exception):
                break
            try:
                out = fn(ctx, calls, out)
            except Exception as e:
                out = e
        return out
