# src/fastweb3/middleware.py
"""Provider-level middleware interfaces.

Middlewares are installed on `fastweb3.provider.Provider` instances and
allow observing or transforming requests and responses.

The public protocol is `ProviderMiddleware`.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Protocol

from .errors import RPCError

if TYPE_CHECKING:
    from .provider import Provider, _BatchCall


@dataclass
class MiddlewareContext:
    """Per-request scratchpad passed to middleware hooks.

    This exists so a middleware can store per-request state in
    `MiddlewareContext.state` during `ProviderMiddleware.before_request()` and
    reuse it during `ProviderMiddleware.after_request()` or
    `ProviderMiddleware.on_exception()`.

    Attributes:
        state: An unstructured dictionary for middleware-owned state. Keys
            should be stable and unique per middleware (using ``self`` is fine).
    """

    state: dict[object, Any]


class ProviderMiddleware(Protocol):
    """Provider-level middleware interface.

    Middlewares are installed per provider instance via
    `fastweb3.provider.Provider.add_middleware()`.

    All hooks are optional. The provider checks for hook existence via
    `getattr()`.

    Execution order for middlewares ``[A, B, C]`` in attach order:

    * ``before_request``: ``A -> B -> C``
    * core request: provider routing/endpoint calls happen here
    * ``after_request``: ``C -> B -> A``
    * ``on_exception``: ``C -> B -> A`` (only when the core raises)

    Important distinction:

    * Per-call JSON-RPC failures inside a batch are returned as
      `fastweb3.errors.RPCError` items in the ``results`` list and flow
      through ``after_request``.
    * Transport-wide failures (timeouts, connection errors, HTTP errors, etc.)
      raise exceptions (usually `fastweb3.errors.TransportError`) and
      trigger ``on_exception``.

    Typical patterns:

    1. Caching / dedupe / batch shaping
       * Implement ``before_request`` to drop/rewrite calls.
       * Store a mapping in ``ctx.state``.
       * Implement ``after_request`` to expand/rewrite results back.

    2. Observability (logging/metrics/tracing)
       * Implement ``before_request`` to record a start time.
       * Implement ``after_request`` to record duration and sizes.
       * Optionally implement ``on_exception`` to record failures.

    3. Recovery on transport failures (advanced)
       * Implement ``on_exception`` to retry or fall back.
       * If you can recover, return a ``results`` list aligned 1:1 with the
         ``calls`` list the provider attempted to execute.
       * If you cannot recover, return or raise the exception to propagate.

    Constraints:

    * ``before_request(ctx, calls)`` receives canonical call objects
      (``_BatchCall``) and may return a new list.
    * For single-call ``Provider.request()``, the middleware chain must leave
      exactly one call; transforming it into many calls is an error.
    * ``after_request(ctx, calls, results)`` receives executed ``calls``
      (post-``before_request``) and results aligned to those calls.
    * ``on_exception(ctx, calls, exc)`` is invoked in reverse attach order until
      one middleware returns recovered results or the chain ends.
    """

    def before_request(
        self, ctx: MiddlewareContext, calls: list["_BatchCall"]
    ) -> list["_BatchCall"]:
        """Hook executed before the provider makes the request(s).

        Args:
            ctx: Per-request context.
            calls: Canonical call objects. Middleware may return a new list to
                change what the provider executes.

        Returns:
            The call list to execute.
        """
        ...

    def after_request(
        self,
        ctx: MiddlewareContext,
        calls: list["_BatchCall"],
        results: list[Any | RPCError],
    ) -> list[Any | RPCError]:
        """Hook executed after the provider receives results.

        Args:
            ctx: Per-request context.
            calls: The executed call list (post-``before_request``).
            results: Results aligned to ``calls``. Elements are either values
                or `fastweb3.errors.RPCError` instances.

        Returns:
            The results list to return to the caller.
        """
        ...

    def on_exception(
        self,
        ctx: MiddlewareContext,
        calls: list["_BatchCall"],
        exc: Exception,
    ) -> list[Any | RPCError] | Exception:
        """Hook executed when the provider raises an exception.

        Args:
            ctx: Per-request context.
            calls: The call list that was attempted.
            exc: The exception raised by the provider.

        Returns:
            Either a recovered results list aligned to ``calls`` or an
            exception to propagate.
        """
        ...


DefaultMiddlewareFactory = Callable[["Provider"], ProviderMiddleware | None]
DefaultMiddlewareSpec = ProviderMiddleware | type[ProviderMiddleware] | DefaultMiddlewareFactory

_default_lock = threading.Lock()
_default_specs: list[DefaultMiddlewareSpec] = []


def register_default_middleware(spec: DefaultMiddlewareSpec, *, prepend: bool = False) -> None:
    """Register a default middleware applied to newly-created providers.

    This is global process state.

    Args:
        spec: One of:

            * Middleware instance: the same instance is attached to every
              provider.
            * Middleware class: instantiated once per provider.
            * Factory: ``factory(provider) -> middleware | None`` (return ``None``
              to skip).

        prepend: If ``True``, insert the spec at the front of the default list.

    Notes:
        Defaults are applied to each provider in registration order. This does
        not affect already-instantiated providers.
    """
    with _default_lock:
        if prepend:
            _default_specs.insert(0, spec)
        else:
            _default_specs.append(spec)


def clear_default_middlewares() -> None:
    """Remove all registered default middleware specs."""
    with _default_lock:
        _default_specs.clear()


def list_default_middlewares() -> list[DefaultMiddlewareSpec]:
    """Return a snapshot of registered default middleware specs."""
    with _default_lock:
        return list(_default_specs)


def _resolve_middleware(
    spec: DefaultMiddlewareSpec, provider: "Provider"
) -> ProviderMiddleware | None:
    # Class -> instantiate per provider
    if isinstance(spec, type):
        return spec()  # type: ignore[call-arg]

    # Instance -> reuse (caller responsibility if they want per-provider instances)
    if not callable(spec):
        return spec  # type: ignore[return-value]

    # Callable -> treat as factory(provider) -> middleware|None
    return spec(provider)  # type: ignore[misc]


def _apply_default_middlewares(provider: "Provider") -> None:
    """Attach registered default middlewares to a new provider.

    Called by ``Provider.__init__``.

    The attachment happens in two passes:

    1. Resolve all eligible middlewares without mutating the provider.
    2. Add them in order.

    This prevents factories from observing side effects from earlier additions
    during the same provider initialization.
    """
    with _default_lock:
        specs = list(_default_specs)

    resolved: list[ProviderMiddleware] = []
    for spec in specs:
        mw = _resolve_middleware(spec, provider)
        if mw is None:
            continue
        resolved.append(mw)

    for mw in resolved:
        provider.add_middleware(mw)
