import threading
import time

import pytest

from fastweb3.deferred import Handle, deferred_response


def _assert_has_creation_note(exc: BaseException) -> None:
    # Python 3.11+ stores exception notes on __notes__
    notes = getattr(exc, "__notes__", None)
    assert notes, "Expected exception to have __notes__"
    joined = "\n".join(notes)
    assert "Deferred value was created at" in joined
    assert "most recent call last" in joined


def test_bg_sets_value_happy_path():
    def bg(h: Handle) -> None:
        h.set_value(123)

    p = deferred_response(bg)
    assert int(p) == 123


def test_format_func_applied():
    def bg(h: Handle) -> None:
        h.set_value("5")

    p = deferred_response(bg, format_func=int)
    assert p + 1 == 6  # proxy resolves to int( "5" )


def test_ref_func_runs_when_value_unset_and_only_once():
    ref_calls = 0
    ref_entered = threading.Event()

    def bg(h: Handle) -> None:
        # intentionally do NOT set value
        return

    def ref(h: Handle) -> None:
        nonlocal ref_calls
        ref_calls += 1
        ref_entered.set()
        h.set_value("ok")

    p = deferred_response(bg, ref_func=ref)

    # First access triggers ref and resolves
    assert str(p) == "ok"
    assert ref_entered.wait(0.5)
    assert ref_calls == 1

    # Subsequent accesses should not re-run ref
    assert p.upper() == "OK"
    assert ref_calls == 1


def test_ref_func_not_called_if_bg_already_set_value():
    ref_calls = 0

    def bg(h: Handle) -> None:
        h.set_value("done")

    def ref(h: Handle) -> None:
        nonlocal ref_calls
        ref_calls += 1
        h.set_value("ref-set")

    p = deferred_response(bg, ref_func=ref)

    assert str(p) == "done"
    assert ref_calls == 0


def test_bg_exception_is_raised_with_creation_note():
    boom = RuntimeError("boom")

    def bg(h: Handle) -> None:
        raise boom

    p = deferred_response(bg)

    with pytest.raises(RuntimeError) as ei:
        # any access should raise
        str(p)

    exc = ei.value
    assert exc is boom  # same instance
    _assert_has_creation_note(exc)


def test_ref_exception_is_raised_with_creation_note():
    boom = ValueError("ref boom")

    def bg(h: Handle) -> None:
        # bg completes without setting a value
        return

    def ref(h: Handle) -> None:
        raise boom

    p = deferred_response(bg, ref_func=ref)

    with pytest.raises(ValueError) as ei:
        str(p)

    exc = ei.value
    assert exc is boom
    _assert_has_creation_note(exc)


def test_format_func_exception_is_stored_and_raised_with_creation_note():
    boom = TypeError("bad format")

    def bg(h: Handle) -> None:
        h.set_value("x")

    def fmt(_raw):
        raise boom

    p = deferred_response(bg, format_func=fmt)

    with pytest.raises(TypeError) as ei:
        str(p)

    exc = ei.value
    assert exc is boom
    _assert_has_creation_note(exc)


def test_creation_note_added_only_once_per_exception_instance():
    boom = RuntimeError("boom-once")

    def bg(h: Handle) -> None:
        raise boom

    h = Handle(bg)

    # First raise adds a note
    with pytest.raises(RuntimeError) as ei1:
        h.get_value()
    exc1 = ei1.value
    notes1 = list(getattr(exc1, "__notes__", []))
    assert len(notes1) >= 1
    _assert_has_creation_note(exc1)

    # Second raise should not add additional notes to same exception instance
    with pytest.raises(RuntimeError) as ei2:
        h.get_value()
    exc2 = ei2.value
    assert exc2 is boom
    notes2 = list(getattr(exc2, "__notes__", []))
    assert notes2 == notes1


def test_never_set_value_raises_attribute_error_with_creation_note():
    def bg(h: Handle) -> None:
        # finishes without setting a value
        return

    h = Handle(bg)

    with pytest.raises(AttributeError) as ei:
        h.get_value()

    exc = ei.value
    assert "Deferred value was not set" in str(exc)
    _assert_has_creation_note(exc)


def test_set_value_twice_raises():
    def bg(h: Handle) -> None:
        h.set_value(1)
        with pytest.raises(Exception, match="Value already set"):
            h.set_value(2)

    # constructing handle runs bg in background; we want the asserts inside bg to execute
    h = Handle(bg)
    # ensure bg finished
    assert h.get_value() == 1


def test_set_exc_first_wins_and_blocks_later_set_value():
    first = RuntimeError("first")
    second = RuntimeError("second")

    def bg(h: Handle) -> None:
        h.set_exc(first)
        h.set_exc(second)  # should be ignored

        # once exc is set, set_value should raise that exc
        with pytest.raises(RuntimeError) as ei:
            h.set_value(123)
        assert ei.value is first

    h = Handle(bg)

    with pytest.raises(RuntimeError) as ei2:
        h.get_value()
    assert ei2.value is first
    _assert_has_creation_note(ei2.value)


def test_get_value_waits_for_bg_event():
    started = threading.Event()
    release = threading.Event()

    def bg(h: Handle) -> None:
        started.set()
        release.wait(1.0)
        h.set_value("ok")

    h = Handle(bg)

    assert started.wait(0.5)

    # In another thread, call get_value; it should block until we release.
    got = {}

    def reader():
        got["v"] = h.get_value()

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    time.sleep(0.05)
    assert "v" not in got  # should still be blocked

    release.set()
    t.join(timeout=1.0)
    assert got["v"] == "ok"


def test_handle_requires_bg_or_ref():
    # bg_func=None and ref_func=None should raise immediately
    with pytest.raises(ValueError, match="Must set one of bg_func, ref_func"):
        Handle(None)

    with pytest.raises(ValueError, match="Must set one of bg_func, ref_func"):
        Handle(bg_func=None, ref_func=None)


def test_no_bg_sets_event_immediately_and_ref_resolves():
    ref_calls = 0
    ref_entered = threading.Event()

    def ref(h: Handle) -> None:
        nonlocal ref_calls
        ref_calls += 1
        ref_entered.set()
        h.set_value("ok")

    h = Handle(bg_func=None, ref_func=ref)

    # New behavior: if there's no bg_func, event is set immediately
    assert h.event.is_set()

    # And get_value should not block; it will run ref once and resolve
    assert h.get_value() == "ok"
    assert ref_entered.wait(0.5)
    assert ref_calls == 1

    # Subsequent access should not re-run ref
    assert h.get_value() == "ok"
    assert ref_calls == 1
