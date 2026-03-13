#!/usr/bin/env python

import os

import iot_daemonize
import iot_daemonize.configuration as configuration

from .core.logging_config import configure_logging
from .core.constants import DEFAULT_RESTART_DELAY, DEFAULT_HEALTH_CHECK_INTERVAL
from .core.config_schema import validate_config
from .core.bridge import SolarLogBridge


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


def main():
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

    bridge = SolarLogBridge(config)
    iot_daemonize.daemon.add_task(bridge.run)

    iot_daemonize.run()


if __name__ == "__main__":
    main()
