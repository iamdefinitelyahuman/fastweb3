from __future__ import annotations

from typing import Any, Mapping, Protocol, TypeAlias

JSONObj: TypeAlias = Mapping[str, Any]
JSONPayload: TypeAlias = JSONObj | list[JSONObj]
JSONResp: TypeAlias = dict[str, Any] | list[dict[str, Any]]


class Transport(Protocol):
    """
    Wire-level transport. JSON in/out.

    v0: sync request only.
    Supports single (dict) and batch (list[dict]) payloads.
    """

    def send(self, payload: JSONPayload) -> JSONResp: ...

    def close(self) -> None: ...
