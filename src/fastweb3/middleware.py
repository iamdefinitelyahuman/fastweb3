# src/fastweb3/middleware.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from .errors import RPCError

if TYPE_CHECKING:
    from .provider import _BatchCall


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
