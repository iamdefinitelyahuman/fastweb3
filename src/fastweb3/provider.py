from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from .endpoint import Endpoint, Formatter
from .errors import AllEndpointsFailed, NoEndpoints, RPCError, TransportError
from .transport import HTTPTransport, Transport


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.05
    retry_on_rpc_error: bool = False


def _default_is_retryable_exc(exc: Exception) -> bool:
    return isinstance(exc, TransportError)


TransportFactory = Callable[[str], Transport]


class Provider:
    """
    Routes requests across multiple endpoints.

    v0:
      - reads: round-robin + retry/failover
      - writes: primary only
    Scaffolding:
      - per-thread pinning context (for future batching determinism)
    """

    def __init__(
        self,
        urls: list[str],
        *,
        retry_policy_read: RetryPolicy | None = None,
        retry_policy_write: RetryPolicy | None = None,
        is_retryable_exc: Callable[[Exception], bool] = _default_is_retryable_exc,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        if not urls:
            raise NoEndpoints("No endpoints provided")

        self._mk_transport = transport_factory or (lambda url: HTTPTransport(url))
        self._endpoints = [Endpoint(u, transport=self._mk_transport(u)) for u in urls]

        self._rr = 0
        self._tls = threading.local()

        self.retry_policy_read = retry_policy_read or RetryPolicy(
            max_attempts=min(3, len(self._endpoints))
        )
        self.retry_policy_write = retry_policy_write or RetryPolicy(max_attempts=1)
        self.is_retryable_exc = is_retryable_exc

    def close(self) -> None:
        for e in self._endpoints:
            e.close()

    def _primary(self) -> Endpoint:
        return self._endpoints[0]

    def _pick_rr(self) -> Endpoint:
        e = self._endpoints[self._rr % len(self._endpoints)]
        self._rr = (self._rr + 1) % (1 << 30)
        return e

    def _get_pinned(self) -> Optional[Endpoint]:
        return getattr(self._tls, "pinned", None)

    @contextmanager
    def pin(self, *, kind: str = "read") -> Iterator[Endpoint]:
        """
        Pin this thread to a single endpoint for deterministic routing.
        Intended for future batching contexts.

        kind="read" pins to the current RR endpoint.
        kind="write" pins to primary (conservative).
        """
        if kind not in ("read", "write"):
            raise ValueError("kind must be 'read' or 'write'")

        prev = self._get_pinned()
        if kind == "write":
            chosen = self._primary()
        else:
            chosen = self._pick_rr()

        self._tls.pinned = chosen
        try:
            yield chosen
        finally:
            self._tls.pinned = prev

    def request(
        self,
        method: str,
        params: list[Any] | tuple[Any, ...],
        *,
        kind: str = "read",
        formatter: Formatter | None = None,
    ) -> Any:
        if kind not in ("read", "write"):
            raise ValueError("kind must be 'read' or 'write'")

        pinned = self._get_pinned()
        if pinned is not None:
            # If pinned, do no failover here; the whole point is determinism.
            # Later you can decide if you want pinned+retry-on-same-endpoint.
            return pinned.request(method, params, formatter=formatter)

        if kind == "write":
            try:
                return self._primary().request(method, params, formatter=formatter)
            except Exception as exc:
                raise AllEndpointsFailed(exc) from exc

        policy = self.retry_policy_read
        last_exc: Exception | None = None
        tried: set[int] = set()

        for attempt in range(1, max(1, policy.max_attempts) + 1):
            ep = self._pick_rr()
            idx = self._endpoints.index(ep)
            if idx in tried and len(tried) < len(self._endpoints):
                for j, candidate in enumerate(self._endpoints):
                    if j not in tried:
                        ep = candidate
                        idx = j
                        break
            tried.add(idx)

            try:
                return ep.request(method, params, formatter=formatter)
            except RPCError as exc:
                last_exc = exc
                if policy.retry_on_rpc_error and attempt < policy.max_attempts:
                    time.sleep(policy.backoff_seconds)
                    continue
                raise
            except Exception as exc:
                last_exc = exc
                if self.is_retryable_exc(exc) and attempt < policy.max_attempts:
                    time.sleep(policy.backoff_seconds)
                    continue
                break

        raise AllEndpointsFailed(last_exc)
