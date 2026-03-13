#!/usr/bin/env python

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import iot_daemonize
import iot_daemonize.configuration as configuration

from .core.solar_log_client import SolarLogClient
from .core.logging_config import configure_logging
from .core.constants import (
    MAX_REQUEST_FAILURES,
    DEFAULT_RESTART_DELAY,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    MAX_DEVICES_DISCOVERY,
    EXIT_CODE_RESTART_REQUIRED,
    STARTUP_DATA,
    POLLING_DATA,
    FAST_POLL_DATA,
    HISTORIC_DATA,
)
from .core.config_schema import validate_config
from .core.mqtt_publisher import MQTTPublisher
from .core.data_processor import DataProcessor
from .core.exceptions import AccessDeniedError
from .core.orchestrator import (
    get_forecast_data as orchestrator_get_forecast_data,
    health_check as orchestrator_health_check,
)


config = None

# Global instances
solar_log_client: Optional[SolarLogClient] = None
mqtt_publisher: Optional[MQTTPublisher] = None
data_processor: Optional[DataProcessor] = None


def create_config():
    """Create MqttDaemonConfiguration with solarlog-specific args."""
    cfg = configuration.MqttDaemonConfiguration(
        program='solarlog2mqtt',
        description='A Solar Log to MQTT bridge'
    )

    # MQTT settings not in base config
    cfg.add_config_arg('mqtt_clientid', flags='--mqtt_clientid', default='solarlog2mqtt',
                       help='The clientid to send to the MQTT server. Default is solarlog2mqtt.')
    cfg.add_config_arg('mqtt_topic', flags='--mqtt_topic', default='solarlog',
                       help='The base topic to publish MQTT messages. Default is solarlog.')

    # Config file
    cfg.add_config_arg('config', flags=['-c', '--config'], default='/etc/solarlog2mqtt.conf',
                       help='The path to the config file. Default is /etc/solarlog2mqtt.conf.')

    # Solar Log settings
    cfg.add_config_arg('solarlog_host', flags=['-s', '--solarlog_host'], default='192.168.1.100',
                       help='The hostname or IP address of the Solar Log device. Default is 192.168.1.100')
    cfg.add_config_arg('solarlog_port', flags='--solarlog_port', default=80,
                       help='The port of the Solar Log device. Default is 80')
    cfg.add_config_arg('solarlog_user', flags='--solarlog_user',
                       help='The username for the Solar Log device.')
    cfg.add_config_arg('solarlog_password', flags='--solarlog_password',
                       help='The password for the Solar Log device.')
    cfg.add_config_arg('poll_interval_current', flags='--poll_interval_current', default=30,
                       help='The fast polling interval for current data in seconds. Default is 30')
    cfg.add_config_arg('poll_interval_periodic', flags='--poll_interval_periodic', default=300,
                       help='The regular polling interval for periodic data in seconds. Default is 300')
    cfg.add_config_arg('historic_data', flags='--historic_data', default=False, action='store_true',
                       help='Enable historic data collection.')
    cfg.add_config_arg('inverter_import', flags='--inverter_import', default=True,
                       help='Enable individual inverter import and processing.')
    cfg.add_config_arg('forecast_enabled', flags='--forecast_enabled', default=False, action='store_true',
                       help='Enable external forecast.solar API integration.')
    cfg.add_config_arg('historic_hour', flags='--historic_hour', default=0,
                       help='Hour for daily historic data collection (0-23). Default is 0')
    cfg.add_config_arg('historic_minute', flags='--historic_minute', default=0,
                       help='Minute for daily historic data collection (0-59). Default is 0')
    cfg.add_config_arg('forecast_latitude', flags='--forecast_latitude',
                       help='Latitude for forecast.solar API.')
    cfg.add_config_arg('forecast_longitude', flags='--forecast_longitude',
                       help='Longitude for forecast.solar API.')
    cfg.add_config_arg('forecast_declination', flags='--forecast_declination', default=30,
                       help='Solar panel declination/tilt angle in degrees. Default is 30')
    cfg.add_config_arg('forecast_azimuth', flags='--forecast_azimuth', default=180,
                       help='Solar panel azimuth angle in degrees (180=south). Default is 180')
    cfg.add_config_arg('health_check_interval', flags='--health_check_interval',
                       default=DEFAULT_HEALTH_CHECK_INTERVAL,
                       help='Health check interval in seconds. Default is 0 (disabled).')
    cfg.add_config_arg('restart_delay', flags='--restart_delay', default=DEFAULT_RESTART_DELAY,
                       help='Delay before restart in seconds. Default is 90.')
    cfg.add_config_arg('display_monitoring', flags='--display_monitoring', default=False,
                       action='store_true',
                       help='Enable detailed display status monitoring.')
    cfg.add_config_arg('log_level', flags='--log_level', default='INFO',
                       help='Logging level. Default is INFO.')
    cfg.add_config_arg('log_format', flags='--log_format', default='text',
                       help='Logging format: text or json. Default is text.')

    cfg.parse_args()
    return cfg


def coerce_config_types(cfg):
    """Convert string config values to proper types after parsing."""
    int_keys = [
        'mqtt_port', 'solarlog_port', 'poll_interval_current', 'poll_interval_periodic',
        'historic_hour', 'historic_minute', 'forecast_declination', 'forecast_azimuth',
        'health_check_interval', 'restart_delay',
    ]
    float_keys = ['forecast_latitude', 'forecast_longitude']
    bool_keys = [
        'historic_data', 'inverter_import', 'forecast_enabled', 'display_monitoring',
        'mqtt_tls', 'mqtt_tls_no_verify',
    ]

    for key in int_keys:
        val = getattr(cfg, key, None)
        if val is not None:
            cfg._config_values[key] = int(val)

    for key in float_keys:
        val = getattr(cfg, key, None)
        if val is not None:
            cfg._config_values[key] = float(val)

    for key in bool_keys:
        val = getattr(cfg, key, None)
        if val is None:
            continue
        if isinstance(val, bool):
            continue
        cfg._config_values[key] = str(val).lower() in ('true', '1', 'yes')


# --- Request helper ---

async def make_request(req_data=None):
    """Make a request to the Solar-Log device and dispatch the response.

    Without req_data, checks login status. With req_data, sends the request,
    dispatches to DataProcessor.process_response, and handles failures.
    """
    if not solar_log_client:
        return False

    if req_data is None:
        return await solar_log_client.check_login_status()

    try:
        result = await solar_log_client.request_with_retry(req_data, attempts=3, base_delay=1.0)
        if result is not None:
            await data_processor.process_response(req_data, result)
            return True
        if solar_log_client.request_counter > MAX_REQUEST_FAILURES:
            logging.warning("Too many request failures, initiating restart")
            await restart_bridge("Request failures")
        return False
    except AccessDeniedError:
        logging.warning("Solar Log access denied - initiating restart")
        await restart_bridge("Access denied")
        return False


# --- Startup sequence ---

async def perform_startup_sequence(stop):
    """Retry startup until device tables (739/744) are present, then fetch device info."""
    attempt = 0
    delay = 2
    max_delay = 60
    try:
        while not stop() and (
            not data_processor or not data_processor.device_list or not data_processor.brand_list
        ):
            attempt += 1
            logging.info(f"Startup attempt #{attempt}: requesting device metadata (739/744)")
            try:
                await make_request(STARTUP_DATA)
            except Exception:
                logging.exception("Error during startup make_request")

            await asyncio.sleep(1)

            if data_processor and data_processor.device_list and data_processor.brand_list:
                logging.info("Startup device metadata present (739/744)")
                break

            logging.info(f"Startup metadata not available yet; retrying in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

        if stop():
            return

        try:
            await request_device_info()
        except Exception:
            logging.exception("Error requesting device info in startup")
    except Exception:
        logging.exception("Startup sequence failure")

async def request_device_info():
    try:
        max_devices = data_processor.num_inverters if data_processor and data_processor.num_inverters > 0 else MAX_DEVICES_DISCOVERY
        logging.debug(f"Requesting device info for up to {max_devices} devices")

        inverter_data_array = []
        for i in range(max_devices):
            inverter_data_array.append(f'"{i}":{{"119":null,"162":null}}')

        device_info_request = '{"141":{' + ','.join(inverter_data_array) + '}}'
        logging.debug("Device info request: {}".format(device_info_request))

        await make_request(device_info_request)

    except Exception:
        logging.exception("request_device_info error")


# --- Polling loops ---

async def start_polling(stop):
    try:
        if config.solarlog_user and config.solarlog_password:
            if not await make_request():
                await asyncio.sleep(2)

        if config.inverter_import:
            logging.info("Starting startup sequence (inverter import enabled)")
            await perform_startup_sequence(stop)
            try:
                logging.info("Seeding first periodic poll (777/778/801)")
                await make_request(POLLING_DATA)
            except Exception:
                logging.exception("Error seeding first periodic poll")
        else:
            logging.info("Requesting basic startup data (inverter import disabled)")
            await make_request('{"610":null,"611":null,"617":null,"706":null,"800":{"100":null,"160":null},"801":{"101":null,"102":null}}')
            await asyncio.sleep(1)

        if config.inverter_import:
            logging.info("Inverter import enabled - starting full polling")
            asyncio.create_task(fast_polling_loop(stop))
            asyncio.create_task(regular_polling_loop(stop))
        else:
            logging.info("Inverter import disabled - starting simple polling")
            asyncio.create_task(simple_polling_loop(stop))

        if config.historic_data:
            logging.info(
                "Historic data enabled - collection at {}:{:02d}".format(
                    config.historic_hour, config.historic_minute
                )
            )
            asyncio.create_task(historic_polling_loop(stop))

        if config.forecast_enabled:
            logging.info("External forecast API enabled")
            asyncio.create_task(forecast_polling_loop(stop))

        if getattr(config, 'health_check_interval', 0) > 0:
            logging.info(
                "Health monitoring enabled - check every %s seconds",
                config.health_check_interval,
            )
            asyncio.create_task(health_check_loop(stop))

        while not stop():
            await asyncio.sleep(1)

    except Exception:
        logging.exception("start_polling error")

async def fast_polling_loop(stop):
    while not stop():
        try:
            await make_request(FAST_POLL_DATA)
            await asyncio.sleep(config.poll_interval_current)

        except Exception:
            logging.exception("fast_polling_loop error")
            await asyncio.sleep(10)

async def regular_polling_loop(stop):
    while not stop():
        try:
            await make_request(POLLING_DATA)
            await asyncio.sleep(config.poll_interval_periodic)

        except Exception:
            logging.exception("regular_polling_loop error")
            await asyncio.sleep(30)

async def historic_polling_loop(stop):
    while not stop():
        try:
            now = datetime.now()
            next_run = now.replace(hour=config.historic_hour, minute=config.historic_minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)

            sleep_seconds = (next_run - now).total_seconds()
            logging.info("Historic data scheduled for {}, sleeping {:.1f} hours".format(next_run, sleep_seconds/3600))
            await asyncio.sleep(sleep_seconds)

            if stop():
                break

            logging.info('Getting long term historic data')

            solar_log_model = data_processor.solar_log_model if data_processor else None
            if solar_log_model == 500:
                logging.info("Solar Log model 500 detected - requesting 854 data only")
                await make_request('{"854": null}')
                await asyncio.sleep(2)
                await make_request('/months.json?_=')
                await asyncio.sleep(5)
                await make_request('/years.json?_=')
            else:
                logging.info("Solar Log model {} - requesting full historic data".format(solar_log_model))
                await make_request(HISTORIC_DATA)
                await asyncio.sleep(2)
                await make_request('/months.json?_=')
                await asyncio.sleep(5)
                await make_request('/years.json?_=')

        except Exception:
            logging.exception("historic_polling_loop error")
            await asyncio.sleep(3600)


async def simple_polling_loop(stop):
    while not stop():
        try:
            await make_request('{"801":{"170":null}}')
            await asyncio.sleep(config.poll_interval_current)

        except Exception:
            logging.exception("simple_polling_loop error")
            await asyncio.sleep(10)


async def forecast_polling_loop(stop):
    while not stop():
        try:
            await asyncio.sleep(60)

            while not stop():
                last_power = data_processor.total_power_w if data_processor else None
                await orchestrator_get_forecast_data(
                    config, solar_log_client, mqtt_publisher.publish, total_power_w=last_power
                )

                now = datetime.now()
                next_hour = (now + timedelta(hours=1)).replace(minute=25, second=0, microsecond=0)
                sleep_seconds = (next_hour - now).total_seconds()

                logging.debug(f"Next forecast call at {next_hour}, sleeping {sleep_seconds} seconds")
                await asyncio.sleep(sleep_seconds)

        except Exception:
            logging.exception("forecast_polling_loop error")
            await asyncio.sleep(3600)


async def health_check_loop(stop):
    while not stop():
        try:
            await orchestrator_health_check(mqtt_publisher, solar_log_client, mqtt_publisher.publish)
            await asyncio.sleep(config.health_check_interval)
        except Exception:
            logging.exception("health_check_loop error")
            await asyncio.sleep(60)


# --- Bridge lifecycle ---

async def restart_bridge(reason):
    try:
        logging.warning("Bridge restart initiated due to: {}".format(reason))
        if mqtt_publisher:
            mqtt_publisher.publish('info/connection', False)
            mqtt_publisher.publish('info/restart_reason', reason)

        restart_delay = getattr(config, 'restart_delay', DEFAULT_RESTART_DELAY)
        logging.info("Waiting {} seconds before restart...".format(restart_delay))
        await asyncio.sleep(restart_delay)

        logging.info("Restarting Solar Log bridge...")
        await stop_solarlog()

        os._exit(EXIT_CODE_RESTART_REQUIRED)

    except Exception:
        logging.exception("restart_bridge error")
        await stop_solarlog()
        os._exit(EXIT_CODE_RESTART_REQUIRED)


async def start_solarlog_bridge(stop):
    global solar_log_client, mqtt_publisher, data_processor
    try:
        logging.info("Starting SolarLog2MQTT bridge")

        # Initialize SolarLog client
        solar_log_client = SolarLogClient(
            host=config.solarlog_host,
            port=config.solarlog_port,
            username=config.solarlog_user,
            password=config.solarlog_password
        )
        await solar_log_client.initialize()

        # Login if credentials are provided
        if solar_log_client.user_pass:
            await solar_log_client.login()

        # Initialize MQTT Publisher (connection handled by iot_daemonize framework)
        mqtt_publisher = MQTTPublisher(
            base_topic=config.mqtt_topic,
        )

        # Initialize DataProcessor
        data_processor = DataProcessor(mqtt_publisher)

        # Start data polling and await until loops are stopped
        await start_polling(stop)
        # Cleanup after polling completes (e.g., due to signal)
        await stop_solarlog()

    except Exception:
        logging.exception("start_solarlog_bridge error")
        await stop_solarlog()


async def stop_solarlog():
    """Stop the bridge — only close SolarLogClient; MQTT is handled by framework."""
    logging.info("Stopping SolarLog2MQTT bridge")

    if solar_log_client:
        await solar_log_client.close()


def run_solarlog_bridge(stop):
    """Daemon task function — creates event loop and runs the async bridge."""
    loop = asyncio.new_event_loop()
    loop.run_until_complete(start_solarlog_bridge(stop))


def main():
    global config

    config = create_config()

    if config.config and os.path.isfile(config.config):
        config.parse_config(config.config)

    coerce_config_types(config)
    validate_config(config)

    # Configure logging BEFORE iot_daemonize.init() so the framework's basicConfig() is a no-op
    level_name = getattr(config, 'log_level', 'INFO')
    log_format = getattr(config, 'log_format', 'text')
    verbose = getattr(config, 'verbose', False)
    configure_logging(level_name, log_format, verbose)

    iot_daemonize.init(config, mqtt=True, daemonize=True)

    iot_daemonize.daemon.add_task(run_solarlog_bridge)

    iot_daemonize.run()


if __name__ == "__main__":
    main()
