"""Config schema and validation for solarlog2mqtt (Py 3.12)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import logging

from .exceptions import ConfigError, ValidationError


ALLOWED_TLS: set[str] = {"TLSv1", "TLSv1.1", "TLSv1.2"}
ALLOWED_VERIFY: set[str] = {"CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"}


def _in_range(name: str, val: int, lo: int, hi: int) -> None:
    if not (lo <= val <= hi):
        raise ValidationError(f"{name} must be between {lo} and {hi}, got {val}")


def validate_config(ns: Any) -> None:
    """Validate critical configuration constraints.

    Raises ConfigError/ValidationError on invalid values.
    """
    for name in ("mqtt_port", "solarlog_port"):
        port = getattr(ns, name, None)
        if port is None:
            raise ConfigError(f"Missing required port: {name}")
        _in_range(name, int(port), 1, 65535)

    for name in ("poll_interval_current", "poll_interval_periodic"):
        val = getattr(ns, name, None)
        if val is None:
            raise ConfigError(f"Missing required interval: {name}")
        if int(val) <= 0:
            raise ValidationError(f"{name} must be > 0, got {val}")

    _in_range("historic_hour", int(getattr(ns, "historic_hour", 0)), 0, 23)
    _in_range("historic_minute", int(getattr(ns, "historic_minute", 0)), 0, 59)

    if getattr(ns, "forecast_enabled", False):
        lat = getattr(ns, "forecast_latitude", None)
        lon = getattr(ns, "forecast_longitude", None)
        if lat is None or lon is None:
            raise ConfigError(
                "forecast_enabled requires forecast_latitude and forecast_longitude"
            )
        if not (-90.0 <= float(lat) <= 90.0):
            raise ValidationError(f"forecast_latitude out of range: {lat}")
        if not (-180.0 <= float(lon) <= 180.0):
            raise ValidationError(f"forecast_longitude out of range: {lon}")
        _in_range(
            "forecast_declination", int(getattr(ns, "forecast_declination", 30)), 0, 90
        )
        _in_range("forecast_azimuth", int(getattr(ns, "forecast_azimuth", 180)), 0, 360)

    tls_version = getattr(ns, "mqtt_tls_version", None)
    if tls_version and tls_version not in ALLOWED_TLS:
        logging.warning(
            "Invalid mqtt_tls_version '%s' – clearing to use library default",
            tls_version,
        )
        setattr(ns, "mqtt_tls_version", None)

    verify = getattr(ns, "mqtt_verify_mode", None)
    if verify and verify not in ALLOWED_VERIFY:
        logging.warning(
            "Invalid mqtt_verify_mode '%s' – clearing to use library default", verify
        )
        setattr(ns, "mqtt_verify_mode", None)
