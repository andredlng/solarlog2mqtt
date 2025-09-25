"""MQTT Publisher (Python 3.12).

Encapsulates MQTT client lifecycle and publishing concerns.
"""

from __future__ import annotations

from typing import Any
import time
import logging
import ssl
from enum import StrEnum

import paho.mqtt.client as mqtt


# Back-compat mapping for string inputs
verify_mode = {
    "CERT_NONE": ssl.CERT_NONE,
    "CERT_OPTIONAL": ssl.CERT_OPTIONAL,
    "CERT_REQUIRED": ssl.CERT_REQUIRED,
}

tls_versions = {
    "TLSv1": ssl.PROTOCOL_TLSv1,
    "TLSv1.1": ssl.PROTOCOL_TLSv1_1,
    "TLSv1.2": ssl.PROTOCOL_TLSv1_2,
}


class TLSVersion(StrEnum):
    TLSv1 = "TLSv1"
    TLSv1_1 = "TLSv1.1"
    TLSv1_2 = "TLSv1.2"

    def to_protocol(self) -> int:
        # Map to ssl protocol constants
        if self is TLSVersion.TLSv1:
            return ssl.PROTOCOL_TLSv1
        if self is TLSVersion.TLSv1_1:
            return ssl.PROTOCOL_TLSv1_1
        return ssl.PROTOCOL_TLSv1_2


class VerifyMode(StrEnum):
    CERT_NONE = "CERT_NONE"
    CERT_OPTIONAL = "CERT_OPTIONAL"
    CERT_REQUIRED = "CERT_REQUIRED"

    def to_cert_reqs(self) -> int:
        if self is VerifyMode.CERT_NONE:
            return ssl.CERT_NONE
        if self is VerifyMode.CERT_OPTIONAL:
            return ssl.CERT_OPTIONAL
        return ssl.CERT_REQUIRED


class MQTTPublisher:
    """Publish values to an MQTT broker with optional TLS and auth."""

    def __init__(
        self,
        host: str,
        port: int,
        keepalive: int,
        clientid: str,
        base_topic: str,
        *,
        enable_timestamp: bool = False,
        tls_enabled: bool = False,
        tls_version: TLSVersion | str | None = None,
        verify_mode_name: VerifyMode | str | None = None,
        ca_path: str | None = None,
        tls_no_verify: bool = False,
        username: str | None = None,
        password: str | None = None,
        verbose: bool = False,
    ) -> None:
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.clientid = clientid
        self.base_topic = base_topic
        self.enable_timestamp = enable_timestamp
        self.tls_enabled = tls_enabled
        self.tls_version = tls_version
        self.verify_mode_name = verify_mode_name
        self.ca_path = ca_path
        self.tls_no_verify = tls_no_verify
        self.username = username
        self.password = password
        self.verbose = verbose

        self.client: mqtt.Client | None = None
        self._connected: bool = False

    def initialize(self) -> mqtt.Client:
        """Create and configure the underlying MQTT client.

        Returns the configured `paho.mqtt.client.Client` instance. This method
        does not establish a network connection; call `connect()` followed by
        `start_loop()` to initiate the connection lifecycle.
        """
        logging.debug("Starting MQTT")
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, self.clientid)

        if self.tls_enabled:
            # Resolve verify mode (enum or string)
            if isinstance(self.verify_mode_name, VerifyMode):
                cert_reqs = self.verify_mode_name.to_cert_reqs()
            else:
                cert_reqs = (
                    verify_mode.get(self.verify_mode_name)
                    if self.verify_mode_name
                    else None
                )

            # Resolve TLS version (enum or string)
            if isinstance(self.tls_version, TLSVersion):
                tls_ver = self.tls_version.to_protocol()
            else:
                tls_ver = (
                    tls_versions.get(self.tls_version) if self.tls_version else None
                )

            ca_certs = self.ca_path if self.ca_path else None
            self.client.tls_set(
                ca_certs=ca_certs, cert_reqs=cert_reqs, tls_version=tls_ver
            )
            self.client.tls_insecure_set(self.tls_no_verify)

        if self.verbose:
            self.client.enable_logger()

        if self.username is not None and self.password is not None:
            self.client.username_pw_set(self.username, self.password)

        # Configure automatic reconnect backoff
        try:
            self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        except Exception:
            logging.debug(
                "reconnect_delay_set not available in this client version",
                exc_info=True,
            )

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        return self.client

    def connect(self) -> None:
        """Initiate a connection to the MQTT broker.

        Prefer async connect to enable automatic reconnection. Start the
        network loop via `start_loop()` after calling this method.
        """
        if self.client is None:
            return
        # Use async connect so the network loop manages (re)connects
        try:
            self.client.connect_async(self.host, self.port, self.keepalive)
        except Exception:
            # Fallback to blocking connect if async not available
            logging.debug(
                "connect_async not available; falling back to connect()", exc_info=True
            )
            self.client.connect(self.host, self.port, self.keepalive)

    def start_loop(self) -> None:
        """Start the MQTT network loop in a background thread."""
        if self.client is not None:
            self.client.loop_start()

    def stop_loop(self) -> None:
        """Stop the MQTT network loop if running."""
        if self.client is not None:
            self.client.loop_stop()

    def disconnect(self) -> None:
        """Disconnect from the broker and emit a final connection-status message."""
        if self.client is not None:
            try:
                self.publish("info/connection", False)
            except Exception:
                logging.debug("Publish during disconnect failed", exc_info=True)
            self.client.disconnect()

    def publish(self, topic: str, value: str | int | float | bool) -> None:
        """Publish a value under `base_topic` and optionally a timestamp.

        No exception is raised on failure; errors are logged and the caller
        can inspect `is_connected()` for current connection state.
        """
        if self.client is None:
            return
        try:
            full_topic = f"{self.base_topic}/{topic}"
            logging.debug(
                "Publishing to MQTT - Topic: %s, Value: %s", full_topic, value
            )
            self.client.publish(full_topic, str(value))
            if self.enable_timestamp:
                self.client.publish(f"{full_topic}/timestamp", time.time(), retain=True)
        except Exception:
            logging.exception("MQTT publish error")

    def is_connected(self) -> bool:
        """Return True if the client is currently connected to the broker."""
        return bool(self._connected and self.client and self.client.is_connected())

    # Internal callbacks
    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        reason_code: int,
        properties: Any | None,
    ) -> None:
        """paho-mqtt on_connect callback."""
        logging.info("Connected to MQTT broker with result code %s", reason_code)
        self._connected = True
        try:
            self.publish("info/connection", True)
        except Exception:
            logging.debug("Initial connection publish failed", exc_info=True)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: int,
        properties: Any | None = None,
    ) -> None:
        """paho-mqtt on_disconnect callback."""
        logging.info("Disconnected from MQTT broker with result code %s", reason_code)
        self._connected = False
