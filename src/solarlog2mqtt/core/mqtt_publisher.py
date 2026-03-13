"""MQTT Publisher adapter wrapping iot_daemonize.mqtt_client."""

from __future__ import annotations

import logging

import iot_daemonize


class MQTTPublisher:
    """Publish values to MQTT via the iot_daemonize framework."""

    def __init__(self, base_topic: str, *, enable_timestamp: bool = False) -> None:
        self.base_topic = base_topic.rstrip('/')
        self.enable_timestamp = enable_timestamp

    def publish(self, topic: str, value: str | int | float | bool) -> None:
        """Publish a value under base_topic, delegating to iot_daemonize."""
        if iot_daemonize.mqtt_client is None:
            return
        try:
            full_topic = f"{self.base_topic}/{topic}"
            logging.debug(
                "Publishing to MQTT - Topic: %s, Value: %s", full_topic, value
            )
            iot_daemonize.mqtt_client.publish(full_topic, str(value))
        except Exception:
            logging.exception("MQTT publish error")

    def is_connected(self) -> bool:
        """Return True if the underlying MQTT client is connected."""
        if iot_daemonize.mqtt_client is None:
            return False
        try:
            return iot_daemonize.mqtt_client._mqtt_client.is_connected()
        except Exception:
            return False
