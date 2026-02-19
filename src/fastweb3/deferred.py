import threading

from lazy_object_proxy import Proxy

_UNSET = object()


class Handle:
    def __init__(self, bg_func, ref_func):
        self.lock = threading.Lock()
        self.event = threading.Event()
        self._exc = None
        self._ref_func = ref_func
        self._ref_ran = False
        self._value = _UNSET

        def execute_in_background():
            try:
                bg_func(self)
            except Exception as exc:
                with self.lock:
                    self._exc = exc
            finally:
                self.event.set()

        threading.Thread(target=execute_in_background, daemon=True).start()

    def set_value(self, value):
        with self.lock:
            if self._value is not _UNSET:
                raise Exception("Already set")
            self._value = value

    def get_value(self):
        self.event.wait()

        # First read state under lock
        with self.lock:
            if self._exc is not None:
                raise self._exc

            have_value = self._value is not _UNSET
            should_run_ref = not have_value and self._ref_func is not None and not self._ref_ran

            if should_run_ref:
                self._ref_ran = True
                ref = self._ref_func
            else:
                ref = None

        # Run barrier outside lock
        if ref is not None:
            ref(self)

        # Final check
        with self.lock:
            if self._exc is not None:
                raise self._exc
            if self._value is _UNSET:
                raise AttributeError("Value was not set")
            return self._value


def deferred_response(bg_func, ref_func=None):
    h = Handle(bg_func, ref_func)
    return Proxy(h.get_value)
