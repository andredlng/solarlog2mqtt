"""Solar Log HTTP client (Python 3.12).

Encapsulates HTTP communication with a Solar-Log device using aiohttp.
Owns session lifecycle and provides login, auth-check, and request helpers.
"""

from __future__ import annotations

from typing import Any
import json
import time
import logging
import asyncio
import aiohttp
from .constants import (
    HTTP_TIMEOUT_SECONDS,
    MAX_REQUEST_FAILURES,
    MAX_LOGIN_FAILURES,
)


def _get_standard_http_headers(
    device_address: str, data_token: str, *, banner_hidden: bool = True
) -> dict[str, str]:
    return {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": f"{device_address}/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/80.0.3987.149 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": f"banner_hidden={str(banner_hidden).lower()}; SolarLog={data_token}",
    }


def _get_login_headers() -> dict[str, str]:
    return {"Cookie": "banner_hidden=false"}


def _get_logcheck_headers(data_token: str) -> dict[str, str]:
    return {"Cookie": f"banner_hidden=false; SolarLog={data_token}"}


class SolarLogClient:
    """HTTP client for Solar-Log devices.

    Owns an aiohttp session, handles optional authentication, and exposes
    helpers for JSON requests against the `/getjp` endpoint and static
    `*.json` resources provided by the device.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.session: aiohttp.ClientSession | None = None
        self.data_token: str = ""
        self.user_pass: bool = bool(username and password)
        self.request_counter: int = 0
        self.login_failures: int = 0
        self.device_address: str = self._setup_device_address(host, port)

    async def initialize(self) -> None:
        """Create the underlying aiohttp session."""
        self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Close the underlying aiohttp session, if any."""
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def login(self) -> bool:
        """Login to the device if credentials are provided.

        Returns True on success, False otherwise. Errors are logged and do not
        raise by design to simplify call sites.
        """
        if not self.user_pass or self.session is None:
            return True

        try:
            headers = _get_login_headers()
            logging.debug("Attempting Solar-Log login")
            async with self.session.post(
                f"{self.device_address}/login",
                data=f"u=user&p={self.password}",
                headers=headers,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:
                logging.debug("Login response status: %s", response.status)
                if response.status == 200:
                    set_cookie = response.headers.get("set-cookie", "")
                    if "SolarLog=" in set_cookie:
                        token_start = set_cookie.find("SolarLog=") + 9
                        token_end = set_cookie.find(";", token_start)
                        if token_end == -1:
                            token_end = len(set_cookie)
                        self.data_token = set_cookie[token_start:token_end]
                        logging.debug(
                            "Login successful, token: %s...", self.data_token[:10]
                        )
                        self.login_failures = 0
                        return True
                    logging.warning("Login succeeded but no token cookie found")
                    return False
                logging.warning("Login failed with status: %s", response.status)
                self.login_failures += 1
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logging.exception("Login error")
            self.login_failures += 1
            return False

    async def check_login_status(self) -> bool:
        """Verify current login status if authentication is required."""
        if not self.user_pass or self.session is None:
            return True

        try:
            headers = _get_logcheck_headers(self.data_token)
            logging.debug("Starting LogCheck")
            async with self.session.get(
                f"{self.device_address}/logcheck?",
                headers=headers,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:
                if response.status == 200:
                    body = await response.text()
                    logging.debug("LogCheck response: %s", body)
                    parts = body.split(";")
                    if parts and parts[0] != "0":
                        return True
                    logging.info("Login status not OK; attempting re-login")
                    return await self.login()
                logging.warning("LogCheck HTTP error: %s", response.status)
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logging.exception("LogCheck error")
            return False

    async def make_request(self, request_data: str) -> dict[str, Any] | None:
        """Perform a request and return parsed JSON payload or None."""
        if self.session is None:
            return None

        try:
            if ".json" in request_data:
                headers = _get_standard_http_headers(
                    self.device_address, self.data_token, banner_hidden=True
                )
                url = f"{self.device_address}{request_data}{int(time.time() * 1000)}"
                async with self.session.get(
                    url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS
                ) as response:
                    logging.debug("HTTP GET %s -> %s", url, response.status)
                    if response.status == 200:
                        text = await response.text()
                        try:
                            return json.loads(text)
                        except json.JSONDecodeError:
                            logging.exception(
                                "Failed to decode JSON from .json response"
                            )
                            return None
                    logging.warning("HTTP GET error: %s", response.status)
                    return None
            else:
                headers = _get_standard_http_headers(
                    self.device_address, self.data_token, banner_hidden=False
                )
                post_data = f"token={self.data_token};preval=none;{request_data}"
                logging.debug("POST /getjp data: %s...", post_data[:100])
                async with self.session.post(
                    f"{self.device_address}/getjp",
                    data=post_data,
                    headers=headers,
                    timeout=HTTP_TIMEOUT_SECONDS,
                ) as response:
                    logging.debug("HTTP POST /getjp -> %s", response.status)
                    if response.status == 200:
                        text = await response.text()
                        try:
                            data = json.loads(text)
                            self.request_counter = 0
                            return data
                        except json.JSONDecodeError:
                            logging.exception(
                                "Failed to decode JSON from /getjp response"
                            )
                            return None
                    logging.warning("HTTP POST error: %s", response.status)
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logging.exception("HTTP request error")
            self.request_counter += 1
            if self.request_counter > MAX_REQUEST_FAILURES:
                logging.error("Too many request failures (%s)", self.request_counter)
            return None

    async def request_with_auth_check(self, request_data: str) -> dict[str, Any] | None:
        """Perform a request and transparently ensure authentication."""
        if self.user_pass:
            ok = await self.check_login_status()
            if not ok:
                return None
        return await self.make_request(request_data)

    async def request_with_retry(
        self,
        request_data: str,
        *,
        attempts: int = 3,
        base_delay: float = 1.0,
    ) -> dict[str, Any] | None:
        """Attempt a request multiple times with exponential backoff.

        Returns the first successful JSON payload or None if all attempts fail.
        """
        delay = base_delay
        for i in range(1, max(1, attempts) + 1):
            result = await self.request_with_auth_check(request_data)
            if result is not None:
                return result
            if i < attempts:
                logging.debug(
                    "Request failed; retry %s/%s after %.1fs", i, attempts - 1, delay
                )
                try:
                    await asyncio.sleep(delay)
                except Exception:
                    pass
                delay = min(delay * 2, 30.0)
        return None

    def _setup_device_address(self, host: str, port: int) -> str:
        if host.startswith("http"):
            device_address = host
        else:
            device_address = f"http://{host}:{port}"
        return device_address.rstrip("/")
