#!/usr/bin/env python

import argparse
from argparse import BooleanOptionalAction
import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union

import paho.mqtt.client as mqtt
from .core.solar_log_client import SolarLogClient
from .core.logging_config import configure_logging
from .core.constants import (
    MAX_REQUEST_FAILURES,
    MAX_LOGIN_FAILURES,
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


mqtt_client = None
daemon_args = None
device_ip_address = ''
running = True


# Header helpers moved to solar_log_client.SolarLogClient


from .core.mqtt_publisher import MQTTPublisher
from .core.data_processor import DataProcessor
from .core.orchestrator import (
    get_forecast_data as orchestrator_get_forecast_data,
    health_check as orchestrator_health_check,
)


# HTTP client class moved to solar_log_client.SolarLogClient

# Global instances
solar_log_client: Optional[SolarLogClient] = None
mqtt_publisher: Optional[MQTTPublisher] = None
data_processor: Optional[DataProcessor] = None

## Device discovery and class list management handled in DataProcessor




def setup_device_address(host: str, port: int) -> str:
    """Setup device address from host and port, return the formatted address."""
    if host.startswith('http'):
        device_address = host
    else:
        device_address = f"http://{host}:{port}"
        
    # Remove trailing slash
    device_address = device_address.rstrip('/')
    logging.info("Solar Log device address: {}".format(device_address))
    return device_address


            
        


async def log_check(data_lc: Optional[str] = None) -> bool:
    """Check authentication status and make request if data is provided."""
    global solar_log_client
    
    if not solar_log_client:
        return False
        
    try:
        if data_lc:
            # Make request with automatic auth check and retry/backoff
            result = await solar_log_client.request_with_retry(data_lc, attempts=3, base_delay=1.0)
            if result is not None:
                await process_response(data_lc, result)
                return True
            # Escalate if repeated failures exceed threshold
            if solar_log_client.request_counter > MAX_REQUEST_FAILURES:
                logging.warning("Too many request failures during log_check, initiating restart")
                await restart_bridge("Request failures")
            return False
        else:
            # Just check auth status
            return await solar_log_client.check_login_status()
            
    except Exception:
        logging.exception("Error in log_check")
        return False
            
async def https_request(req_data: str) -> None:
    """Make a request using the SolarLogClient."""
    global solar_log_client
    
    if not solar_log_client:
        return
        
    try:
        result = await solar_log_client.request_with_retry(req_data, attempts=3, base_delay=1.0)
        if result is not None:
            if '.json' in req_data:
                await process_json_response(req_data, result)
            else:
                await process_response(req_data, result)
        else:
            # Check if we need to restart due to too many failures
            if solar_log_client.request_counter > MAX_REQUEST_FAILURES:
                logging.warning("Too many request failures, initiating restart")
                await restart_bridge("Request failures")
                    
    except Exception:
        logging.exception("Error in https_request")

async def perform_startup_sequence():
    """Retry startup until device tables (739/744) are present, then fetch device info.

    Ensures authentication (if needed) before requests and tolerates transient device issues.
    """
    global running, data_processor
    attempt = 0
    delay = 2
    max_delay = 60
    # Reset any prior state so we can detect when tables are populated
    try:
        # Loop until both device_list and brand_list are present and non-empty
        while running and (
            not data_processor or not data_processor.device_list or not data_processor.brand_list
        ):
            attempt += 1
            logging.info(f"Startup attempt #{attempt}: requesting device metadata (739/744)")
            try:
                await log_check(STARTUP_DATA)
            except Exception:
                logging.exception("Error during startup log_check")

            # Give async device info request a moment to complete if triggered
            await asyncio.sleep(1)

            if data_processor and data_processor.device_list and data_processor.brand_list:
                logging.info("Startup device metadata present (739/744)")
                break

            logging.info(f"Startup metadata not available yet; retrying in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

        if not running:
            return

        # One more pass to ensure device info is fetched with the now-present tables
        try:
            await request_device_info()
        except Exception:
            logging.exception("Error requesting device info in startup")
            # Not fatal; classification will retry in next periodic cycle as needed
    except Exception:
        logging.exception("Startup sequence failure")
async def request_device_info():
    global data_processor
    try:
        # If no inverters detected in startup, try to discover up to a safe upper bound
        max_devices = data_processor.num_inverters if data_processor and data_processor.num_inverters > 0 else MAX_DEVICES_DISCOVERY
        logging.debug(f"Requesting device info for up to {max_devices} devices")
            
        # Build dynamic request for device info (141)
        inverter_data_array = []
        for i in range(max_devices):
            inverter_data_array.append(f'"{i}":{{"119":null,"162":null}}')
        
        device_info_request = '{"141":{' + ','.join(inverter_data_array) + '}}'
        logging.debug("Device info request: {}".format(device_info_request))
        
        await https_request(device_info_request)
        
    except Exception:
        logging.exception("process_response error")


async def process_response(req_data, data):
    try:
        logging.debug("Processing data for request: {}...".format(req_data[:10]))
        
        # Handle different request types
        if req_data.startswith('{"152"'):
            await process_startup_data(data)
        elif req_data.startswith('{"141"'):
            await process_device_info_data(data)
        elif req_data.startswith('{"447"'):
            await process_polling_data(data)
        elif req_data.startswith('{"608"'):
            await process_fast_poll_data(data)
        elif req_data.startswith('{"801"'):
            await process_basic_data(data)
        elif req_data.startswith('{"854"'):
            await process_historic_data_response(req_data, data)
            
    except Exception:
        logging.exception("process_json_response error")
async def process_json_response(req_data, data):
    try:
        if req_data.startswith('/months.json'):
            if data_processor:
                await data_processor.process_months_json(data)
        elif req_data.startswith('/years.json'):
            if data_processor:
                await data_processor.process_years_json(data)
    except Exception:
        logging.exception("process_device_info_data error")

        


async def process_startup_data(data: Dict[str, Any]) -> None:
    """Process startup data using the global DataProcessor instance."""
    global data_processor
    
    if data_processor:
        await data_processor.process_startup_data(data)
        
        # No longer syncing legacy globals; functions should rely on data_processor
    else:
        logging.error("No data processor available")
            
async def process_polling_data(data):
    global data_processor
    try:
        logging.debug("Polling data keys: {}".format(list(data.keys())))
        if data_processor:
            await data_processor.process_periodic_poll(data)
        if '447' in data and data_processor:
            await data_processor.process_switch_group_details(data['447'])
        if data_processor:
            block_801 = data.get('801', data.get(801)) if isinstance(data, dict) else None
            json_data = None
            if isinstance(block_801, dict):
                json_data = block_801.get('170', block_801.get(170))
            if isinstance(json_data, dict):
                try:
                    total_power = int(json_data.get(116, json_data.get('116', 0)))
                except Exception:
                    total_power = 0
                publish_to_mqtt('info/totalPower', total_power)
                data_processor.total_power_w = total_power
    except Exception:
        logging.exception("process_basic_data error")
async def check_access_denied(data: Dict[str, Any]) -> bool:
    """Check if access is denied and restart if needed."""
    if '608' in data and data['608']:
        # Check if access is denied
        first_status = None
        if isinstance(data['608'], list) and len(data['608']) > 0:
            first_status = data['608'][0]
        elif isinstance(data['608'], dict):
            first_status = data['608'].get('0', data['608'].get(0, ''))
        if first_status is not None and "DENIED" in str(first_status):
            logging.warning("Solar Log access denied - initiating restart")
            await restart_bridge("Access denied")
            return True
    return False


async def process_fast_poll_data(data):
    try:
        logging.debug("Fast poll data keys: {}".format(list(data.keys())))
        
        # Check if access is denied first
        if await check_access_denied(data):
            return
        
        if data_processor:
            await data_processor.process_fast_poll(data)
                
    except Exception:
        logging.exception("start_polling error")
            
async def process_device_info_data(data):
    global data_processor
    try:
        logging.debug(f"Device info data: {data}")
        
        if '141' in data:
            device_data = data['141']

            # Determine indices to process
            indices = None
            if data_processor and data_processor.num_inverters > 0:
                indices = list(range(data_processor.num_inverters))
            else:
                # Fallback: derive from keys present in 141
                indices_set = set()
                for k in device_data.keys():
                    if isinstance(k, int):
                        indices_set.add(k)
                    elif isinstance(k, str) and k.isdigit():
                        indices_set.add(int(k))
                indices = sorted(indices_set)
                num_detected = len(indices)
                if num_detected > 0 and data_processor:
                    data_processor.num_inverters = num_detected
                    # JS publishes numinv as (numinv - 1)
                    publish_to_mqtt('info/numinv', max(data_processor.num_inverters - 1, 0))

            # Extract device names and info codes - populate DataProcessor
            inv_names: List[str] = []
            infos: List[int] = []

            for i in indices:
                key = str(i)
                if key in device_data:
                    device_entry = device_data[key]

                    # Extract device name (119) and device info code (162)
                    name = device_entry.get('119', f'Inverter_{i}')
                    info_code = device_entry.get('162', 0)

                    inv_names.append(name)
                    infos.append(info_code)

                    logging.debug(f"Device {i}: {name}, Info: {info_code}")
            
            logging.info("Discovered {} devices: {}".format(len(inv_names), inv_names))
            
            # Update DataProcessor with device names
            if data_processor:
                data_processor.inverter_names = inv_names.copy()
                data_processor.device_infos = infos.copy()
            
            # Process device types and classes and publish info
            if data_processor:
                await data_processor.classify_devices()
                await data_processor.publish_device_info()
                    
    except Exception:
        logging.exception("process_device_info_data error")


async def process_basic_data(data):
    try:
        if '801' in data and '170' in data['801']:
            await process_polling_data(data)
    except Exception:
        logging.exception("process_basic_data error")
            
## JSON processors handled by DataProcessor
            
def publish_to_mqtt(topic: str, value: Union[str, int, float, bool]) -> None:
    """Publish a value to MQTT using the global MQTTPublisher instance."""
    global mqtt_publisher
    
    if mqtt_publisher:
        mqtt_publisher.publish(topic, value)
    else:
        # Fallback logging if no publisher available
        logging.warning(f"No MQTT publisher available for topic {topic}: {value}")
            
async def start_polling():
    global daemon_args, running
    try:
        if daemon_args.solarlog_user and daemon_args.solarlog_password:
            if not await log_check():
                await asyncio.sleep(2)

        if daemon_args.inverter_import:
            logging.info("Starting startup sequence (inverter import enabled)")
            await perform_startup_sequence()
            # Seed one periodic poll so SelfCons and INV daysum are available immediately
            try:
                logging.info("Seeding first periodic poll (777/778/801)")
                await log_check(POLLING_DATA)
            except Exception:
                logging.exception("Error seeding first periodic poll")
        else:
            logging.info("Requesting basic startup data (inverter import disabled)")
            # Only request essential system info for simple mode
            await log_check('{"610":null,"611":null,"617":null,"706":null,"800":{"100":null,"160":null},"801":{"101":null,"102":null}}')
            await asyncio.sleep(1)

        if daemon_args.inverter_import:
            logging.info("Inverter import enabled - starting full polling")
            asyncio.create_task(fast_polling_loop())
            asyncio.create_task(regular_polling_loop())
        else:
            logging.info("Inverter import disabled - starting simple polling")
            asyncio.create_task(simple_polling_loop())

        if daemon_args.historic_data:
            logging.info(
                "Historic data enabled - collection at {}:{:02d}".format(
                    daemon_args.historic_hour, daemon_args.historic_minute
                )
            )
            asyncio.create_task(historic_polling_loop())

        if daemon_args.forecast_enabled:
            logging.info("External forecast API enabled")
            asyncio.create_task(forecast_polling_loop())

        if getattr(daemon_args, 'health_check_interval', 0) > 0:
            logging.info(
                "Health monitoring enabled - check every %s seconds",
                daemon_args.health_check_interval,
            )
            asyncio.create_task(health_check_loop())

        while running:
            await asyncio.sleep(1)

    except Exception:
        logging.exception("start_polling error")
            
async def fast_polling_loop():
    global daemon_args, running
    while running:
        try:
            await log_check(FAST_POLL_DATA)
            await asyncio.sleep(daemon_args.poll_interval_current)
            
        except Exception:
            logging.exception("fast_polling_loop error")
            await asyncio.sleep(10)
async def regular_polling_loop():
    global daemon_args, running
    while running:
        try:
            await log_check(POLLING_DATA)
            await asyncio.sleep(daemon_args.poll_interval_periodic)
            
        except Exception:
            logging.exception("regular_polling_loop error")
            await asyncio.sleep(30)
async def historic_polling_loop():
    global daemon_args, running, data_processor
    while running:
        try:
            now = datetime.now()
            next_run = now.replace(hour=daemon_args.historic_hour, minute=daemon_args.historic_minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)

            sleep_seconds = (next_run - now).total_seconds()
            logging.info("Historic data scheduled for {}, sleeping {:.1f} hours".format(next_run, sleep_seconds/3600))
            await asyncio.sleep(sleep_seconds)

            # Execute historic data collection once per day at scheduled time
            if not running:
                break

            logging.info('Getting long term historic data')

            # Model-specific historic data requests (matches solarlog.js logic)
            solar_log_model = data_processor.solar_log_model if data_processor else None
            if solar_log_model == 500:
                logging.info("Solar Log model 500 detected - requesting 854 data only")
                await log_check('{"854": null}')
                await asyncio.sleep(2)
                await https_request('/months.json?_=')
                await asyncio.sleep(5)
                await https_request('/years.json?_=')
            else:
                logging.info("Solar Log model {} - requesting full historic data".format(solar_log_model))
                await log_check(HISTORIC_DATA)  # Full 854+877+878
                await asyncio.sleep(2)
                await https_request('/months.json?_=')
                await asyncio.sleep(5)
                await https_request('/years.json?_=')

        except Exception:
            logging.exception("historic_polling_loop error")
            await asyncio.sleep(3600)



async def process_monthly_data(data):
    pass


async def process_yearly_data(data):
    pass


## Switch group details handled by DataProcessor


## All shims removed; handling consolidated in DataProcessor


async def restart_bridge(reason):
    global daemon_args, running
    try:
        logging.warning("Bridge restart initiated due to: {}".format(reason))
        publish_to_mqtt('info/connection', False)
        publish_to_mqtt('info/restart_reason', reason)
        
        # Stop current operations
        running = False
        
        # Wait configurable delay before restart (matches solarlog.js behavior)
        restart_delay = getattr(daemon_args, 'restart_delay', DEFAULT_RESTART_DELAY)
        logging.info("Waiting {} seconds before restart...".format(restart_delay))
        await asyncio.sleep(restart_delay)
        
        # Restart the bridge
        logging.info("Restarting Solar Log bridge...")
        await stop_solarlog()
        
        # In a real-world scenario, the process manager (systemd, docker, etc.) would restart the process
        # For now, we just exit with a specific code that can be caught by the process manager
        exit(EXIT_CODE_RESTART_REQUIRED)  # Exit code 2 indicates restart required
        
    except Exception:
        logging.exception("health_check error")
        await stop_solarlog()


# health_check moved to orchestrator; use orchestrator_health_check in loop


async def health_check_loop():
    global daemon_args, running
    while running:
        try:
            await orchestrator_health_check(mqtt_publisher, solar_log_client, publish_to_mqtt)
            await asyncio.sleep(daemon_args.health_check_interval)
        except Exception:
            logging.exception("forecast_polling_loop error")
            await asyncio.sleep(60)  # Wait 1 minute on error


async def simple_polling_loop():
    global daemon_args, running
    while running:
        try:
            # Only request basic status data (matches solarlog.js simple mode)
            await log_check('{"801":{"170":null}}')
            await asyncio.sleep(daemon_args.poll_interval_current)
            
        except Exception:
            logging.exception("simple_polling_loop error")
            await asyncio.sleep(10)


async def forecast_polling_loop():
    global daemon_args, running
    while running:
        try:
            # Initial delay
            await asyncio.sleep(60)  # Wait 1 minute before first forecast call
            
            while running:
                last_power = data_processor.total_power_w if data_processor else None
                await orchestrator_get_forecast_data(
                    daemon_args, solar_log_client, publish_to_mqtt, total_power_w=last_power
                )
                
                # Wait until next hour at 25 minutes (matches solarlog.js schedule)
                now = datetime.now()
                next_hour = (now + timedelta(hours=1)).replace(minute=25, second=0, microsecond=0)
                sleep_seconds = (next_hour - now).total_seconds()
                
                logging.debug(f"Next forecast call at {next_hour}, sleeping {sleep_seconds} seconds")
                await asyncio.sleep(sleep_seconds)
                
        except Exception:
            logging.exception("forecast_polling_loop error")
            await asyncio.sleep(3600)  # Wait 1 hour on error


# Forecast helpers moved to solarlog2mqtt.core.orchestrator


def parse_args():
    parser = argparse.ArgumentParser(
        prog='solarlog2mqtt',
        description='A Solar Log to MQTT bridge',
        epilog='Have a lot of fun!')
        
    # MQTT settings
    parser.add_argument('-m', '--mqtt_host', type=str, default='localhost',
                       help='The hostname of the MQTT server. Default is localhost')
    parser.add_argument('--mqtt_port', type=int, default=1883,
                       help='The port of the MQTT server. Default is 1883')
    parser.add_argument('--mqtt_keepalive', type=int, default=30,
                       help='The keep alive interval for the MQTT server connection in seconds. Default is 30')
    parser.add_argument('--mqtt_clientid', type=str, default='solarlog2mqtt',
                       help='The clientid to send to the MQTT server. Default is solarlog2mqtt')
    parser.add_argument('-u', '--mqtt_user', type=str,
                       help='The username for the MQTT server connection.')
    parser.add_argument('-p', '--mqtt_password', type=str,
                       help='The password for the MQTT server connection.')
    parser.add_argument('-t', '--mqtt_topic', type=str, default='solarlog',
                       help='The topic to publish MQTT message. Default is solarlog')
    parser.add_argument('--mqtt_tls', action=BooleanOptionalAction, default=False,
                       help='Use SSL/TLS encryption for MQTT connection.')
    parser.add_argument('--mqtt_tls_version', type=str, default='TLSv1.2',
                       help='The TLS version to use for MQTT. One of TLSv1, TLSv1.1, TLSv1.2. Default is TLSv1.2')
    parser.add_argument('--mqtt_verify_mode', type=str, default='CERT_REQUIRED',
                       help='The SSL certificate verification mode. One of CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED. Default is CERT_REQUIRED')
    parser.add_argument('--mqtt_ssl_ca_path', type=str,
                       help='The SSL certificate authority file to verify the MQTT server.')
    parser.add_argument('--mqtt_tls_no_verify', action=BooleanOptionalAction, default=False,
                       help='Do not verify SSL/TLS constraints like hostname.')
                       
    # Solar Log settings
    parser.add_argument('-s', '--solarlog_host', type=str, default='192.168.1.100',
                       help='The hostname or IP address of the Solar Log device. Default is 192.168.1.100')
    parser.add_argument('--solarlog_port', type=int, default=80,
                       help='The port of the Solar Log device. Default is 80')
    parser.add_argument('--solarlog_user', type=str,
                       help='The username for the Solar Log device (if authentication is enabled).')
    parser.add_argument('--solarlog_password', type=str,
                       help='The password for the Solar Log device (if authentication is enabled).')
    parser.add_argument('--poll_interval_current', type=int, default=30,
                       help='The fast polling interval for current data in seconds. Default is 30')
    parser.add_argument('--poll_interval_periodic', type=int, default=300,
                       help='The regular polling interval for periodic data in seconds. Default is 300')
    parser.add_argument('--historic_data', action=BooleanOptionalAction, default=False,
                       help='Enable historic data collection.')
    parser.add_argument('--inverter_import', action=BooleanOptionalAction, default=True,
                       help='Enable individual inverter import and processing (use --no-inverter_import to disable).')
    parser.add_argument('--forecast_enabled', action=BooleanOptionalAction, default=False,
                       help='Enable external forecast.solar API integration.')
    parser.add_argument('--historic_hour', type=int, default=0,
                       help='Hour for daily historic data collection (0-23). Default is 0')
    parser.add_argument('--historic_minute', type=int, default=0,
                       help='Minute for daily historic data collection (0-59). Default is 0')
    parser.add_argument('--forecast_latitude', type=float,
                       help='Latitude for forecast.solar API (required if forecast enabled)')
    parser.add_argument('--forecast_longitude', type=float,
                       help='Longitude for forecast.solar API (required if forecast enabled)')
    parser.add_argument('--forecast_declination', type=int, default=30,
                       help='Solar panel declination/tilt angle in degrees. Default is 30')
    parser.add_argument('--forecast_azimuth', type=int, default=180,
                       help='Solar panel azimuth angle in degrees (180=south). Default is 180')
                       
    # Monitoring and reliability settings
    parser.add_argument('--health_check_interval', type=int, default=DEFAULT_HEALTH_CHECK_INTERVAL,
                       help='Health check interval in seconds. Default is 0 (disabled, align with JS)')
    parser.add_argument('--restart_delay', type=int, default=DEFAULT_RESTART_DELAY,
                       help='Delay before restart in seconds. Default is 90')
    parser.add_argument('--display_monitoring', action=BooleanOptionalAction, default=False,
                       help='Enable detailed display status monitoring.')
                       
    # General settings
    parser.add_argument('-c', '--config', type=str, default='/etc/solarlog2mqtt.conf',
                       help='The path to the config file. Default is /etc/solarlog2mqtt.conf')
    parser.add_argument('-z', '--timestamp', default=False, action='store_true',
                       help='Publish timestamps for all topics, e.g. for monitoring purposes.')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                       help='Be verbose while running.')
    parser.add_argument('--log-level', type=str, choices=['CRITICAL','ERROR','WARNING','INFO','DEBUG','NOTSET'], default='INFO',
                       help='Logging level (default: INFO). Overridden by --verbose.')
    parser.add_argument('--log-format', type=str, choices=['text','json'], default='text',
                       help='Logging format: text or json (default: text).')
                       
    return parser.parse_args()


def parse_config():
    global daemon_args
    
    if not os.path.isfile(daemon_args.config):
        return
        
    try:
        with open(daemon_args.config, "r") as config_file:
            data = json.load(config_file)
            
            # MQTT settings
            for key in ['mqtt_host', 'mqtt_port', 'mqtt_keepalive', 'mqtt_clientid',
                       'mqtt_user', 'mqtt_password', 'mqtt_topic', 'mqtt_tls',
                       'mqtt_tls_version', 'mqtt_verify_mode', 'mqtt_ssl_ca_path',
                       'mqtt_tls_no_verify']:
                if key in data:
                    value = data[key]
                    if key in ['mqtt_port', 'mqtt_keepalive']:
                        value = int(value)
                    elif key in ['mqtt_tls', 'mqtt_tls_no_verify']:
                        value = str(value).lower() == 'true'
                    setattr(daemon_args, key, value)
                    
            # Solar Log settings
            for key in ['solarlog_host', 'solarlog_port', 'solarlog_user',
                       'solarlog_password', 'poll_interval_current', 'poll_interval_periodic',
                       'historic_data', 'inverter_import', 'forecast_enabled',
                       'historic_hour', 'historic_minute', 'forecast_latitude',
                       'forecast_longitude', 'forecast_declination', 'forecast_azimuth']:
                if key in data:
                    value = data[key]
                    if key in ['solarlog_port', 'poll_interval_current', 'poll_interval_periodic',
                              'historic_hour', 'historic_minute', 'forecast_declination', 'forecast_azimuth']:
                        value = int(value)
                    elif key in ['forecast_latitude', 'forecast_longitude']:
                        value = float(value)
                    elif key in ['historic_data', 'inverter_import', 'forecast_enabled']:
                        value = str(value).lower() == 'true'
                    setattr(daemon_args, key, value)
                    
            # Monitoring and reliability settings
            for key in ['health_check_interval', 'restart_delay']:
                if key in data:
                    setattr(daemon_args, key, int(data[key]))
            for key in ['display_monitoring']:
                if key in data:
                    setattr(daemon_args, key, str(data[key]).lower() == 'true')
                    
            # General settings
            for key in ['timestamp', 'verbose']:
                if key in data:
                    setattr(daemon_args, key, str(data[key]).lower() == 'true')
            for key in ['log_level', 'log_format']:
                if key in data:
                    setattr(daemon_args, key, str(data[key]))
            
            # Validate final values and normalize tolerant fields
            try:
                validate_config(daemon_args)
            except Exception as exc:
                logging.error("Invalid configuration: %s", exc)
                raise
        
    except Exception:
        logging.exception("switch group detailed processing error")


def shutdown(signum, frame):
    global mqtt_publisher, running
    logging.info('Shutdown...')
    # Signal loops to stop
    running = False
    # Stop MQTT promptly to notify consumers
    if mqtt_publisher is not None:
        logging.info('Stopping MQTT')
        mqtt_publisher.disconnect()
        mqtt_publisher.stop_loop()
    logging.info('Bye!')


async def start_solarlog_bridge():
    global daemon_args, mqtt_client, device_ip_address, solar_log_client, mqtt_publisher, data_processor
    try:
        logging.info("Starting SolarLog2MQTT bridge")

        # Setup device address string from host/port
        device_ip_address = setup_device_address(daemon_args.solarlog_host, daemon_args.solarlog_port)
        
        # Initialize SolarLog client
        solar_log_client = SolarLogClient(
            host=daemon_args.solarlog_host,
            port=daemon_args.solarlog_port,
            username=daemon_args.solarlog_user,
            password=daemon_args.solarlog_password
        )
        await solar_log_client.initialize()
        
        # Login if credentials are provided  
        if solar_log_client.user_pass:
            await solar_log_client.login()
        
        # Initialize MQTT Publisher
        mqtt_publisher = MQTTPublisher(
            host=daemon_args.mqtt_host,
            port=daemon_args.mqtt_port,
            keepalive=daemon_args.mqtt_keepalive,
            clientid=daemon_args.mqtt_clientid,
            base_topic=daemon_args.mqtt_topic,
            enable_timestamp=daemon_args.timestamp,
            tls_enabled=daemon_args.mqtt_tls,
            tls_version=daemon_args.mqtt_tls_version,
            verify_mode_name=daemon_args.mqtt_verify_mode,
            ca_path=daemon_args.mqtt_ssl_ca_path,
            tls_no_verify=daemon_args.mqtt_tls_no_verify,
            username=daemon_args.mqtt_user,
            password=daemon_args.mqtt_password,
            verbose=daemon_args.verbose
        )
        
        # Initialize and connect MQTT
        mqtt_client = mqtt_publisher.initialize()
        mqtt_publisher.connect()
        mqtt_publisher.start_loop()
        
        # Initialize DataProcessor
        data_processor = DataProcessor(mqtt_publisher)
        
        # Start data polling and await until loops are stopped
        await start_polling()
        # Cleanup after polling completes (e.g., due to signal)
        await stop_solarlog()
        
    except Exception:
        logging.exception("start_solarlog_bridge error")
        await stop_solarlog()


async def stop_solarlog():
    global mqtt_client, running, solar_log_client, mqtt_publisher
    logging.info("Stopping SolarLog2MQTT bridge")
    running = False
    
    # Stop MQTT Publisher
    if mqtt_publisher:
        mqtt_publisher.disconnect()
        mqtt_publisher.stop_loop()
        
    # Close SolarLog client
    if solar_log_client:
        await solar_log_client.close()
        
    # Nothing else to close here; SolarLogClient owns its session


def main():
    global daemon_args, mqtt_client
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    daemon_args = parse_args()
    parse_config()
    # Configure logging with flexible level/format
    level_name = getattr(daemon_args, 'log_level', 'INFO')
    log_format = getattr(daemon_args, 'log_format', 'text')
    configure_logging(level_name, log_format, daemon_args.verbose)
    
    asyncio.run(start_solarlog_bridge())


if __name__ == "__main__":
    main()
