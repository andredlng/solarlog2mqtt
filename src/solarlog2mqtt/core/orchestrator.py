"""Orchestration helpers: forecast and health checks (Python 3.12)."""

from __future__ import annotations

import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timedelta
from typing import Callable, Protocol

from .constants import HTTP_TIMEOUT_SECONDS


class PublishFn(Protocol):
    def __call__(self, topic: str, value: int | float | bool | str) -> None: ...


async def _process_forecast_response(
    response, daemon_args, publish_fn: PublishFn
) -> None:
    try:
        if response.status == 200:
            forecast_data = await response.json()
            if forecast_data.get("message", {}).get("type") == "success":
                result = forecast_data.get("result", {})
                today = datetime.now().strftime("%Y-%m-%d")
                tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
                wh_today = int(result.get(today, 0))
                wh_tomorrow = int(result.get(tomorrow, 0))
                publish_fn("forecast/today", wh_today)
                publish_fn("forecast/tomorrow", wh_tomorrow)
                publish_fn("info/latitude", daemon_args.forecast_latitude)
                publish_fn("info/longitude", daemon_args.forecast_longitude)
                publish_fn("info/inclination", daemon_args.forecast_declination)
                publish_fn("info/azimuth", daemon_args.forecast_azimuth)
                logging.info(
                    "Forecast - Today: %sWh, Tomorrow: %sWh", wh_today, wh_tomorrow
                )
            else:
                message = forecast_data.get("message", {})
                logging.warning(
                    "Forecast API error: %s", message.get("text", "Unknown error")
                )
        else:
            logging.warning("Forecast API HTTP error: %s", response.status)
    except Exception:
        logging.exception("Error processing forecast response")


async def get_forecast_data(
    daemon_args,
    solar_log_client,
    publish_fn: PublishFn,
    *,
    total_power_w: int | None = None,
) -> None:
    try:
        logging.debug("Getting forecast data from forecast.solar API")
        if not all([daemon_args.forecast_latitude, daemon_args.forecast_longitude]):
            logging.warning(
                "Forecast API requires latitude and longitude configuration"
            )
            return
        total_power_kw = 5.0
        if total_power_w and total_power_w > 0:
            total_power_kw = total_power_w / 1000
            logging.debug(
                "Using stored total power: %sW (%skW)", total_power_w, total_power_kw
            )
        forecast_url = "https://api.forecast.solar/estimate/watthours/day/"
        url_prog = f"{forecast_url}{daemon_args.forecast_latitude}/{daemon_args.forecast_longitude}/{daemon_args.forecast_declination}/{daemon_args.forecast_azimuth}/{total_power_kw}"
        logging.debug("Forecast API request: %s", url_prog)
        sess = (
            solar_log_client.session
            if solar_log_client and solar_log_client.session
            else None
        )
        if sess is None:
            async with aiohttp.ClientSession() as _temp_sess:
                async with _temp_sess.get(url_prog) as response:
                    await _process_forecast_response(response, daemon_args, publish_fn)
        else:
            async with sess.get(url_prog) as response:
                await _process_forecast_response(response, daemon_args, publish_fn)
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
        logging.exception("Forecast API request error")


async def health_check(mqtt_publisher, solar_log_client, publish_fn: PublishFn) -> bool:
    try:
        health_status = {
            "mqtt_connected": (
                mqtt_publisher and mqtt_publisher.is_connected()
                if mqtt_publisher
                else False
            ),
            "solar_client_active": bool(
                solar_log_client
                and solar_log_client.session
                and not solar_log_client.session.closed
            ),
            "device_accessible": True,
        }
        if solar_log_client and solar_log_client.session:
            try:
                async with solar_log_client.session.get(
                    f"{solar_log_client.device_address}/", timeout=HTTP_TIMEOUT_SECONDS
                ) as response:
                    health_status["device_accessible"] = response.status < 400
            except Exception:
                health_status["device_accessible"] = False
        for key, value in health_status.items():
            publish_fn(f"health/{key}", value)
        overall_health = all(health_status.values())
        publish_fn("health/overall", overall_health)
        if not overall_health:
            logging.warning("Health check failed: %s", health_status)
        else:
            logging.debug("Health check passed")
        return overall_health
    except Exception:
        logging.exception("health_check error")
        publish_fn("health/overall", False)
        return False
