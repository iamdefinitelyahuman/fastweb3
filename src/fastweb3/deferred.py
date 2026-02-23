import threading
import traceback
from typing import Any, Callable, Optional

from lazy_object_proxy import Proxy

_UNSET = object()


class Handle:
    def __init__(
        self,
        bg_func: Callable[["Handle"], None],
        *,
        format_func: Optional[Callable[[Any], Any]] = None,
        ref_func: Optional[Callable[["Handle"], None]] = None,
    ) -> None:
        """
        @notice
            Creates a deferred value handle backed by a background task and a
            demand-triggered barrier.

        @param bg_func
            Function executed immediately in a background thread.
            It is responsible for eventually calling `set_value(...)`
            or `set_exc(...)`.

        @param format_func
            Pure function applied inside `set_value(raw)` to transform the
            raw value into its final form. It receives only the raw value and
            must return the formatted value.

        @param ref_func
            Function executed at most once, on first access of the proxy,
            if the value has not yet been set. It may ensure that
            `set_value(...)` is called.
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

        def execute_in_background() -> None:
            try:
                bg_func(self)
            except BaseException as exc:
                self.set_exc(exc)
            finally:
                self.event.set()

        threading.Thread(target=execute_in_background, daemon=True).start()

    def set_exc(self, exc: BaseException) -> None:
        self._add_creation_note(exc)
        with self.lock:
            if self._exc is None:
                self._exc = exc

    def set_value(self, raw_value: Any) -> None:
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
    h = Handle(bg_func, format_func=format_func, ref_func=ref_func)
    return Proxy(h.get_value)
