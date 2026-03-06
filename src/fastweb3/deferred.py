"""Deferred values used for optional background execution.

This module provides a small primitive for returning proxy objects whose value
is computed later.

The main entrypoint is `deferred_response()`, which returns a
``lazy_object_proxy.Proxy`` that blocks on first access until the underlying
value is ready.

These utilities are primarily used internally (e.g., for batched JSON-RPC
requests), but are designed to be safe under concurrent access.
"""

# src/fastweb3/deferred.py
import threading
import traceback
from typing import Any, Callable, Optional

from lazy_object_proxy import Proxy

_UNSET = object()


class Handle:
    """Handle for a deferred value.

    A `Handle` represents a value that will be produced later, either by
    a background task or by a "ref" callback that runs on first access.

    The handle stores either a final value or an exception. Consumers typically
    do not use `Handle` directly; instead, use `deferred_response()`
    which returns a proxy object backed by a handle.
    """

    def __init__(
        self,
        bg_func: Optional[Callable[["Handle"], None]] = None,
        format_func: Optional[Callable[[Any], Any]] = None,
        ref_func: Optional[Callable[["Handle"], None]] = None,
    ) -> None:
        """Create a new handle.

        Exactly one of ``bg_func`` or ``ref_func`` must be provided.

        Args:
            bg_func: If provided, executed immediately in a background thread.
                The function is responsible for eventually calling
                `set_value()` or `set_exc()`.
            format_func: Optional pure function applied to the raw value before
                storing it. If ``format_func`` raises, the handle is marked as
                failed and the exception is re-raised.
            ref_func: If provided, executed at most once on first access of the
                proxy *and only if* the value has not been set yet. This is
                typically used to force a flush or otherwise ensure that a value
                will be produced.

        Raises:
            ValueError: If neither ``bg_func`` nor ``ref_func`` is provided.
        """
        self.lock = threading.Lock()
        self.event = threading.Event()

        self._exc: Optional[BaseException] = None
        self._value: Any = _UNSET

        self._format_func = format_func
        self._ref_func = ref_func
        self._ref_ran = False

        # Capture creation site (strip this __init__ frame)
        self._created_stack = traceback.format_stack()[:-1]

        if bg_func is not None:

            def execute_in_background() -> None:
                try:
                    bg_func(self)
                except BaseException as exc:
                    self.set_exc(exc)
                finally:
                    self.event.set()

            threading.Thread(target=execute_in_background, daemon=True).start()
        else:
            if ref_func is None:
                raise ValueError("Must set one of bg_func, ref_func")
            self.event.set()

    def set_exc(self, exc: BaseException) -> None:
        """Mark the handle as failed.

        Args:
            exc: The exception to store.

        Notes:
            The exception is annotated with a creation-site note the first time
            it is stored.
        """
        self._add_creation_note(exc)
        with self.lock:
            if self._exc is None:
                self._exc = exc

    def set_value(self, raw_value: Any) -> None:
        """Store the handle's final value.

        Args:
            raw_value: Raw value to store. If ``format_func`` was provided,
                it is applied first.

        Raises:
            BaseException: Re-raises any exception produced by ``format_func``.
            Exception: If the value was already set.
        """
        with self.lock:
            if self._exc is not None:
                raise self._exc
            if self._value is not _UNSET:
                raise Exception("Value already set")
            format_func = self._format_func

        try:
            final_value = raw_value if format_func is None else format_func(raw_value)
        except BaseException as exc:
            self.set_exc(exc)
            raise

        with self.lock:
            if self._exc is not None:
                raise self._exc
            if self._value is not _UNSET:
                raise Exception("Value already set")
            self._value = final_value

    def _add_creation_note(self, exc: BaseException) -> None:
        # Add exactly once per exception instance
        if getattr(exc, "_fastweb3_creation_note", False):
            return
        setattr(exc, "_fastweb3_creation_note", True)

        exc.add_note(
            "Deferred value was created at (most recent call last):\n"
            + "".join(self._created_stack)
        )

    def get_value(self) -> Any:
        """Return the stored value, blocking until it is available.

        Returns:
            The resolved value.

        Raises:
            BaseException: If the handle resolved to an exception.
            AttributeError: If the handle was resolved but no value was set.
        """
        self.event.wait()

        # Fast-path: already resolved or already failed
        with self.lock:
            exc = self._exc
            value = self._value
            if exc is not None:
                raise exc
            if value is not _UNSET:
                return value

            # Decide whether to run ref (at most once)
            if self._ref_func is None or self._ref_ran:
                ref = None
            else:
                self._ref_ran = True
                ref = self._ref_func

        if ref is not None:
            try:
                ref(self)
            except BaseException as exc:
                self.set_exc(exc)
                raise

        # Re-check after ref
        with self.lock:
            exc = self._exc
            value = self._value

        if exc is not None:
            raise exc

        if value is _UNSET:
            e = AttributeError("Deferred value was not set")
            self._add_creation_note(e)
            raise e

        return value


def deferred_response(
    bg_func: Callable[[Handle], None],
    *,
    format_func: Optional[Callable[[Any], Any]] = None,
    ref_func: Optional[Callable[[Handle], None]] = None,
) -> Any:
    """Return a proxy whose value is computed later.

    Args:
        bg_func: Background function that computes the value and calls
            `Handle.set_value()` or `Handle.set_exc()`.
        format_func: Optional formatter applied to the raw value.
        ref_func: Optional callback that runs on first proxy access if the
            value is not yet set.

    Returns:
        A ``lazy_object_proxy.Proxy`` that resolves to the final value.
    """
    h = Handle(bg_func, format_func=format_func, ref_func=ref_func)
    return Proxy(h.get_value)
