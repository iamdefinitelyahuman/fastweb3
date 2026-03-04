import threading

import pytest

from fastweb3.deferred import Handle


def test_set_value_raises_if_exc_set_during_formatting() -> None:
    """Covers the race where another thread sets an exception mid-set_value()."""

    fmt_started = threading.Event()
    fmt_release = threading.Event()

    def fmt(x):
        fmt_started.set()
        assert fmt_release.wait(1.0)
        return x

    h = Handle(bg_func=None, format_func=fmt, ref_func=lambda _h: None)

    err = RuntimeError("boom")
    out: dict[str, object] = {}

    def writer() -> None:
        try:
            h.set_value(123)
            out["ok"] = True
        except BaseException as exc:  # pragma: no cover
            out["exc"] = exc

    t = threading.Thread(target=writer)
    t.start()

    assert fmt_started.wait(1.0)
    h.set_exc(err)
    fmt_release.set()

    t.join(timeout=1.0)
    assert not t.is_alive()

    assert "exc" in out
    assert out["exc"] is err

    # Once an exception is set, get_value() should raise it.
    with pytest.raises(RuntimeError) as ei:
        h.get_value()
    assert ei.value is err


def test_set_value_raises_if_other_thread_sets_value_during_formatting() -> None:
    """Covers the race where two writers concurrently call set_value()."""

    fmt_started = threading.Event()
    fmt_release = threading.Event()
    lock = threading.Lock()
    calls = {"n": 0}

    def fmt(x):
        with lock:
            calls["n"] += 1
            n = calls["n"]

        # First call blocks to create a deterministic interleaving; second call
        # returns immediately so the second writer can win the race.
        if n == 1:
            fmt_started.set()
            assert fmt_release.wait(1.0)
        return x

    h = Handle(bg_func=None, format_func=fmt, ref_func=lambda _h: None)

    out: dict[str, object] = {}

    def writer1() -> None:
        try:
            h.set_value("first")
            out["w1"] = "ok"
        except BaseException as exc:
            out["w1_exc"] = exc

    def writer2() -> None:
        try:
            h.set_value("second")
            out["w2"] = "ok"
        except BaseException as exc:  # pragma: no cover
            out["w2_exc"] = exc

    t1 = threading.Thread(target=writer1)
    t1.start()
    assert fmt_started.wait(1.0)

    # Second writer runs while the first is blocked in fmt().
    t2 = threading.Thread(target=writer2)
    t2.start()
    t2.join(timeout=1.0)
    assert not t2.is_alive()

    # Release the first writer; it should observe the value was already set.
    fmt_release.set()
    t1.join(timeout=1.0)
    assert not t1.is_alive()

    assert out.get("w2") == "ok"
    assert isinstance(out.get("w1_exc"), Exception)
    assert "Value already set" in str(out["w1_exc"])

    # Final stored value should be from writer2.
    assert h.get_value() == "second"


def test_ref_can_set_exc_without_raising_and_get_value_surfaces_it() -> None:
    """Covers the path where ref() signals failure via set_exc() and returns."""

    boom = ValueError("ref-failed")

    def ref(h: Handle) -> None:
        h.set_exc(boom)
        # Intentionally *not* raising.

    h = Handle(bg_func=None, ref_func=ref)

    with pytest.raises(ValueError) as ei:
        h.get_value()

    assert ei.value is boom
