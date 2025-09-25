"""Lightweight API response validation helpers (Python 3.12).

These helpers add safe accessors and structural checks for Solar-Log API
payloads. They are intentionally minimal and log-only so callers can decide
what to do when data is missing or malformed.
"""

from __future__ import annotations

from typing import Any, Iterable
import logging


def as_dict(val: Any, *, ctx: str = "") -> dict[str, Any] | None:
    if isinstance(val, dict):
        return val  # type: ignore[return-value]
    logging.debug(
        "Expected dict for %s but got %s", ctx or "<root>", type(val).__name__
    )
    return None


def as_list(val: Any, *, ctx: str = "") -> list[Any] | None:
    if isinstance(val, list):
        return val
    logging.debug(
        "Expected list for %s but got %s", ctx or "<root>", type(val).__name__
    )
    return None


def has_keys(d: dict[str, Any] | None, keys: Iterable[str], *, ctx: str = "") -> bool:
    if d is None:
        logging.debug("Missing object for keys %s in %s", list(keys), ctx or "<root>")
        return False
    missing = [k for k in keys if k not in d]
    if missing:
        logging.debug("Missing keys %s in %s", missing, ctx or "<root>")
        return False
    return True


def get_nested(
    d: dict[str, Any] | None, path: list[str], *, ctx: str = ""
) -> Any | None:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            logging.debug("Missing path %s at %s", "/".join(path), ctx or "<root>")
            return None
        cur = cur[p]
    return cur
