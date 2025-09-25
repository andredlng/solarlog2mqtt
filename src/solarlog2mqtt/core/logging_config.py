"""Logging configuration helpers for solarlog2mqtt (Python 3.12).

Provides a small utility to configure root logging with either text or JSON
format, honoring a desired level and a verbose override.
"""

from __future__ import annotations

import json
import logging
from typing import Any


LEVELS: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: dict[str, Any] = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(
    level_name: str | None = None, fmt: str = "text", verbose: bool = False
) -> None:
    """Configure root logging with the desired level and format.

    - If `verbose` is True, the level is forced to DEBUG.
    - `fmt` can be 'text' or 'json'.
    - If `level_name` is None, defaults to INFO.
    """
    if verbose:
        level = logging.DEBUG
    else:
        level = LEVELS.get((level_name or "INFO").upper(), logging.INFO)

    handler = logging.StreamHandler()
    if fmt == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)
