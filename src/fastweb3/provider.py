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
        urls: list[str] | None = None,
        *,
        retry_policy_read: RetryPolicy | None = None,
        retry_policy_write: RetryPolicy | None = None,
        is_retryable_exc: Callable[[Exception], bool] = _default_is_retryable_exc,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        self._lock = threading.Lock()

        self._mk_transport = transport_factory or (lambda url: HTTPTransport(url))
        self._endpoints: list[Endpoint] = []
        if urls:
            for u in urls:
                self._endpoints.append(Endpoint(u, transport=self._mk_transport(u)))

        self._rr = 0
        self._tls = threading.local()

        self.retry_policy_read = retry_policy_read or RetryPolicy(max_attempts=3)
        self.retry_policy_write = retry_policy_write or RetryPolicy(max_attempts=1)
        self.is_retryable_exc = is_retryable_exc

    def add_url(self, url: str, *, priority: bool = False) -> None:
        """
        Add a new endpoint URL to the provider pool (thread-safe).
        """

        ep = Endpoint(url, transport=self._mk_transport(url))
        with self._lock:
            if priority:
                self._endpoints.insert(0, ep)
            else:
                self._endpoints.append(ep)

    def endpoint_count(self) -> int:
        with self._lock:
            return len(self._endpoints)

    def close(self) -> None:
        # Close a snapshot to avoid holding the lock while closing transports.
        with self._lock:
            eps = list(self._endpoints)
        for e in eps:
            e.close()

    def _snapshot(self) -> list[Endpoint]:
        with self._lock:
            return list(self._endpoints)

    def _primary(self) -> Endpoint:
        with self._lock:
            if not self._endpoints:
                raise NoEndpoints("No endpoints available")
            return self._endpoints[0]

    def _pick_rr_from(self, eps: list[Endpoint]) -> Endpoint:
        # Caller provides a snapshot 'eps' (non-empty).
        with self._lock:
            start = self._rr % len(eps)
            self._rr = (self._rr + 1) % (1 << 30)
        return eps[start]

    def _get_pinned(self) -> Optional[Endpoint]:
        return getattr(self._tls, "pinned", None)

    @contextmanager
    def pin(self, *, kind: str = "read") -> Iterator[Endpoint]:
        """
        Pin this thread to a single endpoint for deterministic routing.
        Intended for future batching contexts.

        kind="read" pins to the current RR endpoint snapshot.
        kind="write" pins to primary (conservative).

        Note: If no endpoints exist yet, this raises NoEndpoints.
        """
        if kind not in ("read", "write"):
            raise ValueError("kind must be 'read' or 'write'")

        prev = self._get_pinned()
        if kind == "write":
            chosen = self._primary()
        else:
            eps = self._snapshot()
            if not eps:
                raise NoEndpoints("No endpoints available")
            chosen = self._pick_rr_from(eps)

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

        # Writes go only to primary.
        if kind == "write":
            try:
                return self._primary().request(method, params, formatter=formatter)
            except Exception as exc:
                raise AllEndpointsFailed(exc) from exc

        eps = self._snapshot()
        if not eps:
            raise NoEndpoints("No endpoints available")

        policy = self.retry_policy_read
        max_attempts = max(1, policy.max_attempts)
        attempts = min(max_attempts, len(eps))

        last_exc: Exception | None = None

        # We rotate through a snapshot to avoid races with dynamic pool mutation.
        # If the pool grows mid-flight, it will be picked up on the next request.
        # If it shrinks (unlikely in v1), this call remains stable.
        for attempt in range(1, attempts + 1):
            # round-robin start point + linear walk across snapshot
            with self._lock:
                start = self._rr % len(eps)
                self._rr = (self._rr + 1) % (1 << 30)
            ep = eps[(start + (attempt - 1)) % len(eps)]

            try:
                return ep.request(method, params, formatter=formatter)
            except RPCError as exc:
                last_exc = exc
                if policy.retry_on_rpc_error and attempt < attempts:
                    time.sleep(policy.backoff_seconds)
                    continue
                raise
            except Exception as exc:
                last_exc = exc
                if self.is_retryable_exc(exc) and attempt < attempts:
                    time.sleep(policy.backoff_seconds)
                    continue
                break

        raise AllEndpointsFailed(last_exc)
