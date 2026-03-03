# src/fastweb3/middleware.py
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Protocol

from .errors import RPCError

if TYPE_CHECKING:
    from .provider import Provider, _BatchCall


@dataclass
class MiddlewareContext:
    """
    @notice
        Per-request scratchpad passed to middleware hooks.

        This exists so a middleware can:
          - compute some mapping/metadata in `before_request(...)`, store it on `ctx.state`,
          - and then use it later in `after_request(...)` (or `on_exception(...)`).

        This avoids storing per-request state on the middleware instance itself
        (which is error-prone if the same middleware object is reused across
        multiple Provider instances or concurrent calls).

    @dev
        `state` is intentionally an unstructured dict:
          - keys SHOULD be stable and unique per middleware (using `self` is fine)
          - values can be anything (mappings, timestamps, counters, etc.)
    """

    state: dict[object, Any]


class ProviderMiddleware(Protocol):
    """
    @notice
        Provider-level middleware interface.

        Middlewares are installed per Provider instance via `Provider.add_middleware(...)`.

        All hooks are OPTIONAL: Provider checks for method existence with `getattr()`.
        Implement only what you need.

    @dev
        Execution order for middlewares [A, B, C] in attach order:

            before_request:  A -> B -> C
            core request:                Provider routing/Endpoint calls happen here
            after_request:   C -> B -> A
            on_exception:    C -> B -> A   (only when the core raises an exception)

        Important distinction:
          - Per-call JSON-RPC failures inside a batch are returned as `RPCError` items
            in the `results` list and will flow through `after_request(...)`.
          - Transport-wide failures (timeouts, connection errors, HTTP errors, etc.)
            raise exceptions (usually `TransportError`) and trigger `on_exception(...)`.

    Typical patterns:

      1) Caching / Dedupe / Batch shaping
         - Implement `before_request` to drop/rewrite calls.
         - Store a mapping in `ctx.state`.
         - Implement `after_request` to expand/rewrite results back to the original shape.

      2) Observability (logging/metrics/tracing)
         - Implement `before_request` to record a start time in `ctx.state`.
         - Implement `after_request` to record duration, sizes, etc.
         - Optionally implement `on_exception` to record failures.

      3) Recovery on transport failures (advanced)
         - Implement `on_exception` to retry or fall back.
         - If you can recover, return a `results` list aligned 1:1 with the `calls`
           list that the Provider attempted to execute.
         - If you cannot recover, return the exception (or raise) to propagate.

    Constraints & best practices:

      - `before_request(ctx, calls)`:
          * receives a list of canonical call objects (`_BatchCall`)
          * may return a NEW list (or the same list) to change what gets executed
          * for `Provider.request()` (single call), the Provider expects the middleware
            chain to leave exactly ONE call; transforming it into many calls is an error.

      - `after_request(ctx, calls, results)`:
          * receives the final executed `calls` list (after all `before_request` hooks)
          * `results` is aligned to `calls` (same length, same order)
          * may return a NEW list of results (usually same length as `calls`),
            or (if your middleware shrank the calls) you can return results expanded
            to the original call list shape using a mapping saved in `ctx.state`.

      - `on_exception(ctx, calls, exc)`:
          * called only when the core raises an exception (e.g., `TransportError`)
          * may return:
              - `results` list (recovered), OR
              - an `Exception` (propagate)
          * if multiple middlewares implement `on_exception`, they are invoked in
            reverse attach order until one returns recovered results, or the chain ends.
    """

    def before_request(
        self, ctx: MiddlewareContext, calls: list["_BatchCall"]
    ) -> list["_BatchCall"]: ...

    def after_request(
        self,
        ctx: MiddlewareContext,
        calls: list["_BatchCall"],
        results: list[Any | RPCError],
    ) -> list[Any | RPCError]: ...

    def on_exception(
        self,
        ctx: MiddlewareContext,
        calls: list["_BatchCall"],
        exc: Exception,
    ) -> list[Any | RPCError] | Exception: ...


DefaultMiddlewareFactory = Callable[["Provider"], ProviderMiddleware | None]
DefaultMiddlewareSpec = ProviderMiddleware | type[ProviderMiddleware] | DefaultMiddlewareFactory

_default_lock = threading.Lock()
_default_specs: list[DefaultMiddlewareSpec] = []


def register_default_middleware(spec: DefaultMiddlewareSpec, *, prepend: bool = False) -> None:
    """
    @notice
        Register a default middleware spec applied to newly-created Provider instances.

    @dev
        This is global process state.

        `spec` forms:
          - middleware instance:
              register_default_middleware(MyMiddleware())
              NOTE: the same instance will be attached to every Provider that is created.

          - middleware class:
              register_default_middleware(MyMiddleware)
              A new instance will be created for each Provider.

          - factory:
              def factory(provider) -> ProviderMiddleware | None: ...
              register_default_middleware(factory)
              The factory runs once per Provider init. Return None to skip.

        Ordering:
          - Defaults are applied to each Provider in registration order.
          - If prepend=True, the spec is inserted at the front of the defaults list.

        This does not affect already-instantiated Providers.
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
    """
    @dev
        Called by Provider.__init__ to attach global defaults to the new instance.

        Two-pass behavior:
          1) resolve all eligible middlewares without mutating provider._middlewares
          2) add them in order

        This avoids factories observing side effects from earlier additions during the same init.
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
