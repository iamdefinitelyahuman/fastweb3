from __future__ import annotations

from typing import Any, Mapping, Protocol


class Transport(Protocol):
    """
    Wire-level transport. JSON in/out.

    v0: single-request only (dict payload -> dict response).
    Later: add batch support (list payload -> list response) and/or async variants.
    """

    def send(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def close(self) -> None: ...
