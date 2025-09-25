"""Custom exceptions for solarlog2mqtt (Python 3.12).

Defines a small hierarchy to represent common error categories across
HTTP, MQTT, configuration, validation, and external integrations.
"""

from __future__ import annotations

from dataclasses import dataclass


class SolarLogError(Exception):
    """Base exception for solarlog2mqtt."""


class AuthError(SolarLogError):
    """Authentication or authorization failed."""


@dataclass(slots=True)
class RequestError(SolarLogError):
    """HTTP request failed or returned unexpected status."""

    message: str
    status: int | None = None
    url: str | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        base = self.message
        if self.status is not None:
            base += f" (status={self.status})"
        if self.url:
            base += f" url={self.url}"
        return base


class DecodeError(SolarLogError):
    """Response could not be parsed/decoded as expected (e.g., JSON)."""


class ConfigError(SolarLogError):
    """Configuration invalid or missing required values."""


class ValidationError(SolarLogError):
    """Input or API response validation failed."""


class MQTTError(SolarLogError):
    """MQTT connection or publish error."""


class ForecastError(SolarLogError):
    """External forecast API error or rate-limit condition."""
