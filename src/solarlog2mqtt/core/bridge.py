"""SolarLog MQTT bridge: owns client + processor, runs polling loops (Python 3.12)."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta

from .solar_log_client import SolarLogClient
from .data_processor import DataProcessor
from .constants import (
    MAX_REQUEST_FAILURES,
    MAX_DEVICES_DISCOVERY,
    DEFAULT_RESTART_DELAY,
    EXIT_CODE_RESTART_REQUIRED,
    STARTUP_DATA,
    POLLING_DATA,
    FAST_POLL_DATA,
    HISTORIC_DATA,
)
from .exceptions import AccessDeniedError
from .orchestrator import (
    get_forecast_data as orchestrator_get_forecast_data,
    health_check as orchestrator_health_check,
)


class SolarLogBridge:
    """Encapsulates the Solar-Log ↔ MQTT bridge lifecycle and polling."""

    def __init__(self, config) -> None:
        self.config = config
        self.solar_log_client: SolarLogClient | None = None
        self.data_processor: DataProcessor | None = None

    # --- Request helper ---

    async def make_request(self, req_data: str | None = None) -> bool:
        """Make a request to the Solar-Log device and dispatch the response."""
        if not self.solar_log_client:
            return False

        if req_data is None:
            return await self.solar_log_client.check_login_status()

        try:
            result = await self.solar_log_client.request_with_retry(req_data, attempts=3, base_delay=1.0)
            if result is not None:
                await self.data_processor.process_response(req_data, result)
                return True
            if self.solar_log_client.request_counter > MAX_REQUEST_FAILURES:
                logging.warning("Too many request failures, initiating restart")
                await self.restart_bridge("Request failures")
            return False
        except AccessDeniedError:
            logging.warning("Solar Log access denied - initiating restart")
            await self.restart_bridge("Access denied")
            return False

    # --- Startup sequence ---

    async def perform_startup_sequence(self, stop) -> None:
        """Retry startup until device tables (739/744) are present, then fetch device info."""
        attempt = 0
        delay = 2
        max_delay = 60
        try:
            while not stop() and (
                not self.data_processor
                or not self.data_processor.device_list
                or not self.data_processor.brand_list
            ):
                attempt += 1
                logging.info(f"Startup attempt #{attempt}: requesting device metadata (739/744)")
                try:
                    await self.make_request(STARTUP_DATA)
                except Exception:
                    logging.exception("Error during startup make_request")

                await asyncio.sleep(1)

                if self.data_processor and self.data_processor.device_list and self.data_processor.brand_list:
                    logging.info("Startup device metadata present (739/744)")
                    break

                logging.info(f"Startup metadata not available yet; retrying in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

            if stop():
                return

            try:
                await self.request_device_info()
            except Exception:
                logging.exception("Error requesting device info in startup")
        except Exception:
            logging.exception("Startup sequence failure")

    async def request_device_info(self) -> None:
        try:
            max_devices = (
                self.data_processor.num_inverters
                if self.data_processor and self.data_processor.num_inverters > 0
                else MAX_DEVICES_DISCOVERY
            )
            logging.debug(f"Requesting device info for up to {max_devices} devices")

            inverter_data_array = []
            for i in range(max_devices):
                inverter_data_array.append(f'"{i}":{{"119":null,"162":null}}')

            device_info_request = '{"141":{' + ','.join(inverter_data_array) + '}}'
            logging.debug("Device info request: {}".format(device_info_request))

            await self.make_request(device_info_request)

        except Exception:
            logging.exception("request_device_info error")

    # --- Generic polling loop ---

    async def _polling_loop(
        self,
        stop,
        name: str,
        req_data: str,
        interval: float,
        error_interval: float,
    ) -> None:
        """Generic polling loop that replaces the copy-pasted fast/regular/simple loops."""
        while not stop():
            try:
                await self.make_request(req_data)
                await asyncio.sleep(interval)
            except Exception:
                logging.exception("%s error", name)
                await asyncio.sleep(error_interval)

    # --- Polling orchestration ---

    async def start_polling(self, stop) -> None:
        cfg = self.config
        try:
            if cfg.solarlog_user and cfg.solarlog_password:
                if not await self.make_request():
                    await asyncio.sleep(2)

            if cfg.inverter_import:
                logging.info("Starting startup sequence (inverter import enabled)")
                await self.perform_startup_sequence(stop)
                try:
                    logging.info("Seeding first periodic poll (777/778/801)")
                    await self.make_request(POLLING_DATA)
                except Exception:
                    logging.exception("Error seeding first periodic poll")
            else:
                logging.info("Requesting basic startup data (inverter import disabled)")
                await self.make_request(
                    '{"610":null,"611":null,"617":null,"706":null,"800":{"100":null,"160":null},"801":{"101":null,"102":null}}'
                )
                await asyncio.sleep(1)

            if cfg.inverter_import:
                logging.info("Inverter import enabled - starting full polling")
                asyncio.create_task(
                    self._polling_loop(stop, "fast_polling_loop", FAST_POLL_DATA, cfg.poll_interval_current, 10)
                )
                asyncio.create_task(
                    self._polling_loop(stop, "regular_polling_loop", POLLING_DATA, cfg.poll_interval_periodic, 30)
                )
            else:
                logging.info("Inverter import disabled - starting simple polling")
                asyncio.create_task(
                    self._polling_loop(stop, "simple_polling_loop", '{"801":{"170":null}}', cfg.poll_interval_current, 10)
                )

            if cfg.historic_data:
                logging.info(
                    "Historic data enabled - collection at {}:{:02d}".format(
                        cfg.historic_hour, cfg.historic_minute
                    )
                )
                asyncio.create_task(self.historic_polling_loop(stop))

            if cfg.forecast_enabled:
                logging.info("External forecast API enabled")
                asyncio.create_task(self.forecast_polling_loop(stop))

            if getattr(cfg, 'health_check_interval', 0) > 0:
                logging.info(
                    "Health monitoring enabled - check every %s seconds",
                    cfg.health_check_interval,
                )
                asyncio.create_task(self.health_check_loop(stop))

            while not stop():
                await asyncio.sleep(1)

        except Exception:
            logging.exception("start_polling error")

    async def historic_polling_loop(self, stop) -> None:
        cfg = self.config
        while not stop():
            try:
                now = datetime.now()
                next_run = now.replace(
                    hour=cfg.historic_hour, minute=cfg.historic_minute, second=0, microsecond=0
                )
                if next_run <= now:
                    next_run += timedelta(days=1)

                sleep_seconds = (next_run - now).total_seconds()
                logging.info(
                    "Historic data scheduled for {}, sleeping {:.1f} hours".format(next_run, sleep_seconds / 3600)
                )
                await asyncio.sleep(sleep_seconds)

                if stop():
                    break

                logging.info('Getting long term historic data')

                solar_log_model = self.data_processor.solar_log_model if self.data_processor else None
                if solar_log_model == 500:
                    logging.info("Solar Log model 500 detected - requesting 854 data only")
                    await self.make_request('{"854": null}')
                    await asyncio.sleep(2)
                    await self.make_request('/months.json?_=')
                    await asyncio.sleep(5)
                    await self.make_request('/years.json?_=')
                else:
                    logging.info("Solar Log model {} - requesting full historic data".format(solar_log_model))
                    await self.make_request(HISTORIC_DATA)
                    await asyncio.sleep(2)
                    await self.make_request('/months.json?_=')
                    await asyncio.sleep(5)
                    await self.make_request('/years.json?_=')

            except Exception:
                logging.exception("historic_polling_loop error")
                await asyncio.sleep(3600)

    async def forecast_polling_loop(self, stop) -> None:
        while not stop():
            try:
                await asyncio.sleep(60)

                while not stop():
                    last_power = self.data_processor.total_power_w if self.data_processor else None
                    await orchestrator_get_forecast_data(
                        self.config, self.solar_log_client, self.data_processor.publish,
                        total_power_w=last_power,
                    )

                    now = datetime.now()
                    next_hour = (now + timedelta(hours=1)).replace(minute=25, second=0, microsecond=0)
                    sleep_seconds = (next_hour - now).total_seconds()

                    logging.debug(f"Next forecast call at {next_hour}, sleeping {sleep_seconds} seconds")
                    await asyncio.sleep(sleep_seconds)

            except Exception:
                logging.exception("forecast_polling_loop error")
                await asyncio.sleep(3600)

    async def health_check_loop(self, stop) -> None:
        while not stop():
            try:
                await orchestrator_health_check(self.solar_log_client, self.data_processor.publish)
                await asyncio.sleep(self.config.health_check_interval)
            except Exception:
                logging.exception("health_check_loop error")
                await asyncio.sleep(60)

    # --- Bridge lifecycle ---

    async def restart_bridge(self, reason: str) -> None:
        try:
            logging.warning("Bridge restart initiated due to: {}".format(reason))
            if self.data_processor:
                self.data_processor.publish('info/connection', False)
                self.data_processor.publish('info/restart_reason', reason)

            restart_delay = getattr(self.config, 'restart_delay', DEFAULT_RESTART_DELAY)
            logging.info("Waiting {} seconds before restart...".format(restart_delay))
            await asyncio.sleep(restart_delay)

            logging.info("Restarting Solar Log bridge...")
            await self.stop()

            os._exit(EXIT_CODE_RESTART_REQUIRED)

        except Exception:
            logging.exception("restart_bridge error")
            await self.stop()
            os._exit(EXIT_CODE_RESTART_REQUIRED)

    async def start(self, stop) -> None:
        """Initialize client + processor, run polling, clean up."""
        try:
            logging.info("Starting SolarLog2MQTT bridge")

            self.solar_log_client = SolarLogClient(
                host=self.config.solarlog_host,
                port=self.config.solarlog_port,
                username=self.config.solarlog_user,
                password=self.config.solarlog_password,
            )
            await self.solar_log_client.initialize()

            if self.solar_log_client.user_pass:
                await self.solar_log_client.login()

            self.data_processor = DataProcessor(base_topic=self.config.mqtt_topic)

            await self.start_polling(stop)
            await self.stop()

        except Exception:
            logging.exception("start_solarlog_bridge error")
            await self.stop()

    async def stop(self) -> None:
        """Stop the bridge — only close SolarLogClient; MQTT is handled by framework."""
        logging.info("Stopping SolarLog2MQTT bridge")
        if self.solar_log_client:
            await self.solar_log_client.close()

    def run(self, stop) -> None:
        """Daemon task function — creates event loop and runs the async bridge."""
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.start(stop))
