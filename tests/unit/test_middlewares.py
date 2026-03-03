# tests/unit/test_middlewares.py
from __future__ import annotations

import pytest

import fastweb3.middleware as mw_mod
from fastweb3.provider import Provider


@pytest.fixture(autouse=True)
def _clear_default_registry() -> None:
    mw_mod.clear_default_middlewares()
    yield
    mw_mod.clear_default_middlewares()


class _MWBase:
    """Tiny middleware objects for registry tests (no hooks needed)."""


class MWInstance(_MWBase):
    pass


class MWClass(_MWBase):
    def __init__(self) -> None:
        self.created = True


def test_default_registry_register_list_clear() -> None:
    assert mw_mod.list_default_middlewares() == []

    inst = MWInstance()
    mw_mod.register_default_middleware(inst)

    specs = mw_mod.list_default_middlewares()
    assert specs == [inst]

    mw_mod.clear_default_middlewares()
    assert mw_mod.list_default_middlewares() == []


def test_default_registry_prepend_order() -> None:
    a = MWInstance()
    b = MWInstance()
    c = MWInstance()

    mw_mod.register_default_middleware(a)
    mw_mod.register_default_middleware(b)
    mw_mod.register_default_middleware(c, prepend=True)

    specs = mw_mod.list_default_middlewares()
    assert specs == [c, a, b]


def test_default_registry_class_instantiated_per_provider() -> None:
    mw_mod.register_default_middleware(MWClass)

    p1 = Provider([])
    p2 = Provider([])

    # Each provider should get its own instance.
    assert len(p1._middlewares) == 1  # type: ignore[attr-defined]
    assert len(p2._middlewares) == 1  # type: ignore[attr-defined]
    assert p1._middlewares[0] is not p2._middlewares[0]  # type: ignore[attr-defined]
    assert isinstance(p1._middlewares[0], MWClass)  # type: ignore[attr-defined]
    assert isinstance(p2._middlewares[0], MWClass)  # type: ignore[attr-defined]


def test_default_registry_instance_is_reused_across_providers() -> None:
    inst = MWInstance()
    mw_mod.register_default_middleware(inst)

    p1 = Provider([])
    p2 = Provider([])

    assert len(p1._middlewares) == 1  # type: ignore[attr-defined]
    assert len(p2._middlewares) == 1  # type: ignore[attr-defined]
    assert p1._middlewares[0] is inst  # type: ignore[attr-defined]
    assert p2._middlewares[0] is inst  # type: ignore[attr-defined]


def test_default_registry_factory_can_skip_by_returning_none() -> None:
    def factory(provider: Provider):
        return None

    mw_mod.register_default_middleware(factory)

    p = Provider([])
    assert len(p._middlewares) == 0  # type: ignore[attr-defined]


def test_default_registry_factory_two_pass_no_mid_init_side_effects() -> None:
    seen_counts: list[int] = []

    class MW(_MWBase):
        pass

    def factory(provider: Provider):
        # The registry applies defaults in two passes; during resolution we should
        # NOT see other default middlewares already added.
        seen_counts.append(len(provider._middlewares))  # type: ignore[attr-defined]
        return MW()

    mw_mod.register_default_middleware(factory)
    mw_mod.register_default_middleware(factory)

    p = Provider([])

    assert seen_counts == [0, 0]
    assert len(p._middlewares) == 2  # type: ignore[attr-defined]
