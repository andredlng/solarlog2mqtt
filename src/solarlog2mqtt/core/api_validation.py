"""Lightweight API response validation helpers (Python 3.12).

These helpers add safe accessors and structural checks for Solar-Log API
payloads. They are intentionally minimal and log-only so callers can decide
what to do when data is missing or malformed.
"""

from __future__ import annotations

from typing import Any, Iterable, Protocol


class PublishFn(Protocol):
    def __call__(self, topic: str, value: int | float | bool | str) -> None: ...


def safe_get(container: Any, key: Any, default: Any = None) -> Any:
    """Type-agnostic indexed access for list, dict (int or str key), or None."""
    if container is None:
        return default
    if isinstance(container, list):
        return container[key] if 0 <= key < len(container) else default
    if isinstance(container, dict):
        if key in container:
            return container[key]
        return container.get(str(key), default)
    return default


def as_dict(val: Any, *, ctx: str = "") -> dict[str, Any] | None:
    if isinstance(val, dict):
        return val  # type: ignore[return-value]
    return None


def as_list(val: Any, *, ctx: str = "") -> list[Any] | None:
    if isinstance(val, list):
        return val
    return None


def has_keys(d: dict[str, Any] | None, keys: Iterable[str], *, ctx: str = "") -> bool:
    if d is None:
        return False
    missing = [k for k in keys if k not in d]
    if missing:
        return False
    return True


def get_nested(
    d: dict[str, Any] | None, path: list[str], *, ctx: str = ""
) -> Any | None:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur
