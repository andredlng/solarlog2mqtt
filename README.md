# solarlog2mqtt - A Solar Log to MQTT Bridge allowing unidirectional data transfer

I've created this project to read data from Solar Log devices and publish it to MQTT. This works well for integrating Solar Log data into home automation systems like [HomeAssistant](https://home-assistant.io/).

It is quite simple and does what it's name says: It works as a bridge between Solar Log and MQTT translating data from Solar Log to MQTT messages.

## Installation

### Native installation with Python venv

Requires Python 3.12.

Philosophy is to install it under /usr/local/lib/solarlog2mqtt and control it via systemd.

```bash
cd /usr/local/lib
git clone https://c0d3.sh/andre/solarlog2mqtt.git
cd solarlog2mqtt
./install
```

The `install` script creates a virtual python environment using the `venv` module.
All required libraries are installed automatically.
Depending on your system this may take some time.

## Configuration

The configuration is located in `/etc/solarlog2mqtt.conf`.

Each configuration option is also available as command line argument.

- copy `solarlog2mqtt.conf.example`
- configure as you like

| option                   | default              | arguments                  | comment                                                                                |
|--------------------------|----------------------|----------------------------|----------------------------------------------------------------------------------------|
| `mqtt_host`              | 'localhost'          | `-m`, `--mqtt_host`        | The hostname of the MQTT server.                                                       |
| `mqtt_port`              | 1883                 | `--mqtt_port`              | The port of the MQTT server.                                                           |
| `mqtt_keepalive`         | 30                   | `--mqtt_keepalive`         | The keep alive interval for the MQTT server connection in seconds.                     |
| `mqtt_clientid`          | 'solarlog2mqtt'      | `--mqtt_clientid`          | The clientid to send to the MQTT server.                                               |
| `mqtt_user`              | -                    | `-u`, `--mqtt_user`        | The username for the MQTT server connection.                                           |
| `mqtt_password`          | -                    | `-p`, `--mqtt_password`    | The password for the MQTT server connection.                                           |
| `mqtt_topic`             | 'solarlog'           | `-t`, `--mqtt_topic`       | The topic to publish MQTT message.                                                     |
| `mqtt_tls`               | -                    | `--mqtt_tls`               | Use SSL/TLS encryption for MQTT connection.                                            |
| `mqtt_tls_version`       | 'TLSv1.2'            | `--mqtt_tls_version`       | The TLS version to use for MQTT. One of TLSv1, TLSv1.1, TLSv1.2.                       |
| `mqtt_verify_mode`       | 'CERT_REQUIRED'      | `--mqtt_verify_mode`       | The SSL certificate verification mode. One of CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED. |
| `mqtt_ssl_ca_path`       | -                    | `--mqtt_ssl_ca_path`       | The SSL certificate authority file to verify the MQTT server.                          |
| `mqtt_tls_no_verify`     | -                    | `--mqtt_tls_no_verify`     | Do not verify SSL/TLS constraints like hostname.                                       |
| `solarlog_host`          | '192.168.1.100'      | `-s`, `--solarlog_host`    | The address of the Solar Log device.                                                   |
| `solarlog_port`          | 80                   | `--solarlog_port`          | The port of the Solar Log device.                                                      |
| `solarlog_user`          | -                    | `--solarlog_user`          | The username for the Solar Log device connection.                                      |
| `solarlog_password`      | -                    | `--solarlog_password`      | The password for the Solar Log device connection.                                      |
| `poll_interval_periodic` | 300                  | `--poll_interval_periodic` | The polling interval for periodic data in seconds.                                     |
| `poll_interval_current`  | 30                   | `--poll_interval_current`  | The fast polling interval for current data in seconds.                                 |
| `historic_data`          | -                    | `--historic_data`          | Enable historic data collection.                                                       |
| `inverter_import`        | true                 | `--inverter_import/--no_inverter_import` | Enable individual inverter import.                          |
| `health_check_interval`  | 0                    | `--health_check_interval`  | Health check interval in seconds (0 disables).                                         |
| `restart_delay`          | 90                   | `--restart_delay`          | Delay before restart after critical failure.                                           |
| `forecast_enabled`       | -                    | `--forecast_enabled`       | Enable external forecast.solar API integration.                                        |
| `forecast_latitude`      | -                    | `--forecast_latitude`      | Latitude for forecast.solar API.                                                       |
| `forecast_longitude`     | -                    | `--forecast_longitude`     | Longitude for forecast.solar API.                                                      |
| `forecast_declination`   | 30                   | `--forecast_declination`   | Panel tilt in degrees.                                                                 |
| `forecast_azimuth`       | 180                  | `--forecast_azimuth`       | Panel azimuth in degrees (180=south).                                                  |
| `timestamp`              | -                    | `-z`, `--timestamp`        | Publish timestamps for all topics, e.g. for monitoring purposes.                       |
| `verbose`                | -                    | `-v`, `--verbose`          | Be verbose while running (forces DEBUG).                                               |
| `log_level`              | INFO                 | `--log-level`              | Logging level: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET.                          |
| `log_format`             | text                 | `--log-format`             | Logging format: text or json.                                                          |
| -                        | '/etc/solarlog2mqtt.conf' | `-c`, `--config`      | The path to the config file.                                                           |

### Solar Log

Currently, only HTTP access to Solar Log devices is supported.
It may become more in the future, if I found testing environments with different setups.
Feel free to add other connection types and open a pull request for this.

### Data Points

The bridge automatically discovers and publishes available data from your Solar Log device.

The default operating mode is to read from the Solar Log device and publish the values to MQTT.

### Publishing

All values are published using a hierarchical topic structure and the MQTT topic.

So, the current production is published to `solarlog/status/pac` and consumption to `solarlog/status/consumption`.

## Running solarlog2mqtt

Use the provided run script or invoke via the venv:

```bash
./run -c solarlog2mqtt.conf
# or
./venv/bin/python solarlog2mqtt -c solarlog2mqtt.conf --log-level INFO
```

For service management, use [systemd](https://systemd.io/) or your preferred supervisor.

## Support

I have not the time (yet) to provide professional support for this project.
But feel free to submit issues and PRs, I'll check for it and honor your contributions.

## Acknowledgments

- Inspired by Gerrit's ([c0d3.sh](https://c0d3.sh/smarthome)|[GitHub](https://github.com/gbeine)) smarthome repositories

## License

The whole project is licensed under BSD-3-Clause license. Stay fair.
