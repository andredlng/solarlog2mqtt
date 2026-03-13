"""Solar-Log data processing (Python 3.12).

Implements startup, fast/periodic polling, and publishes values via the
provided MQTT publisher.  Device registry and historic processing are
delegated to dedicated modules.
"""

from __future__ import annotations

from typing import Any
import logging
from datetime import datetime, timedelta

import iot_daemonize

from .api_validation import as_dict, safe_get
from .constants import (
    MAX_SWITCH_GROUPS,
    DEFAULT_SETPOINT_DAILY_DIVISOR,
    DAYS_TO_CHECK_HISTORY,
)
from .device_registry import DeviceRegistry
from .exceptions import AccessDeniedError
from .historic_processor import HistoricProcessor
from .request_types import RequestType, classify_request


class DataProcessor:
    """Handles processing of Solar-Log device data and publishing to MQTT."""

    def __init__(self, base_topic: str = "solarlog") -> None:
        self.base_topic = base_topic.rstrip('/')
        self.devices = DeviceRegistry()

        # Track latest daily production figures for ratio calculations
        self.last_yield_day = 0
        self.last_yield_yesterday = 0

        # Fallback cache for yesterday self-consumption across month boundary
        self.last_selfcons_yesterday_fallback = 99
        self.last_selfcons_ratio_yesterday_fallback = 99
        # Last known total power (W) for forecast helper
        self.total_power_w: int = 0

        # Sub-processors
        self.historic = HistoricProcessor(self.publish, self.devices)

    # --- Convenience accessors for bridge/orchestrator compatibility ---

    @property
    def num_inverters(self) -> int:
        return self.devices.num_inverters

    @property
    def inverter_names(self) -> list[str]:
        return self.devices.inverter_names

    @property
    def device_list(self) -> dict[str, Any] | None:
        return self.devices.device_list

    @property
    def brand_list(self) -> dict[str, Any] | None:
        return self.devices.brand_list

    @property
    def solar_log_model(self) -> int | None:
        return self.devices.solar_log_model

    @staticmethod
    def _selfcons_ratio(
        selfcons: int | float, total: int | float, *, unit_factor: float = 1.0
    ) -> float:
        """Self-consumption as percentage with 1 decimal. Returns 0 if total <= 0."""
        if total <= 0:
            return 0
        return round(((selfcons * unit_factor) / total) * 1000) / 10

    def publish(self, topic: str, value: str | int | float | bool) -> None:
        """Publish a value to MQTT via iot_daemonize.mqtt_client."""
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

    async def process_response(self, req_data: str, data: Any) -> None:
        """Unified dispatcher for all Solar-Log response types."""
        logging.debug("Processing data for request: %s...", req_data[:10])
        match classify_request(req_data):
            case RequestType.MONTHS_JSON:
                await self.historic.process_months_json(data)
            case RequestType.YEARS_JSON:
                await self.historic.process_years_json(data)
            case RequestType.STARTUP:
                await self.process_startup_data(data)
            case RequestType.DEVICE_INFO:
                await self.process_device_info(data)
            case RequestType.PERIODIC_POLL:
                await self.process_periodic_poll(data)
            case RequestType.FAST_POLL:
                await self.process_fast_poll(data)
            case RequestType.SIMPLE_POLL:
                if '801' in data and isinstance(data.get('801'), dict) and '170' in data['801']:
                    await self.process_periodic_poll(data)
            case RequestType.HISTORIC:
                await self.historic.process_historic_response(req_data, data)

    async def process_device_info(self, data: dict[str, Any]) -> None:
        """Process device info response (block 141) — inverter names and info codes."""
        try:
            logging.debug("Device info data: %s", data)
            if '141' not in data:
                return

            device_data = data['141']

            if self.devices.num_inverters > 0:
                indices = list(range(self.devices.num_inverters))
            else:
                indices_set: set[int] = set()
                for k in device_data.keys():
                    if isinstance(k, int):
                        indices_set.add(k)
                    elif isinstance(k, str) and k.isdigit():
                        indices_set.add(int(k))
                indices = sorted(indices_set)
                num_detected = len(indices)
                if num_detected > 0:
                    self.devices.num_inverters = num_detected
                    self.publish('info/numinv', max(self.devices.num_inverters - 1, 0))

            inv_names: list[str] = []
            infos: list[int] = []

            for i in indices:
                key = str(i)
                if key in device_data:
                    device_entry = device_data[key]
                    name = device_entry.get('119', f'Inverter_{i}')
                    info_code = device_entry.get('162', 0)
                    inv_names.append(name)
                    infos.append(info_code)
                    logging.debug("Device %s: %s, Info: %s", i, name, info_code)

            logging.info("Discovered %s devices: %s", len(inv_names), inv_names)

            self.devices.inverter_names = inv_names.copy()
            self.devices.device_infos = infos.copy()

            await self.devices.classify_devices()
            await self.publish_device_info()

        except Exception:
            logging.exception("process_device_info error")

    async def process_startup_data(self, data: dict[str, Any]) -> None:
        """Process startup data and extract device information."""
        try:
            logging.debug("Startup data keys: %s", list(data.keys()))

            await self._process_device_system_info(data)
            await self._process_sd_card_info(data)
            await self.devices.process_device_discovery(data)
            # publish numinv after discovery
            self.publish("info/numinv", max(self.devices.num_inverters - 1, 0))
            await self.devices.process_switch_groups(data)
            await self.devices.process_battery_info(data)

            if "152" in data and "161" in data and "162" in data:
                try:
                    await self._process_setpoint_data(
                        data["152"], data["161"], data["162"]
                    )
                except Exception as exc:
                    logging.warning("Error processing setpoint data: %s", exc)
            else:
                logging.debug(
                    "Setpoint data (152/161/162) not available in startup data"
                )

        except Exception:
            logging.exception("Startup data processing error")

    async def publish_device_info(self) -> None:
        """Publish per-device metadata (class/type/brand) to MQTT."""
        try:
            inv_names = self.devices.inverter_names
            types = self.devices.device_types
            brands = self.devices.device_brands
            classes = self.devices.device_classes
            logging.debug("Publishing device info for %s devices", len(inv_names))
            logging.debug("Device names: %s", inv_names)
            logging.debug("Device types: %s", types)
            logging.debug("Device brands: %s", brands)
            logging.debug("Device classes: %s", classes)

            for i, name in enumerate(inv_names):
                if i < len(classes):
                    self.publish(f"INV/{name}/deviceclass", classes[i])
                if i < len(types):
                    self.publish(f"INV/{name}/devicetype", types[i])
                if i < len(brands):
                    self.publish(f"INV/{name}/devicebrand", brands[i])

            logging.info("Published device info for %s devices", len(inv_names))
        except Exception:
            logging.exception("Failed to publish device info")

    async def _process_device_system_info(self, data: dict[str, Any]) -> None:
        """Publish basic device/system information present in startup payload."""
        if "610" in data:
            self.publish("info/RTOS", data["610"])
        if "611" in data:
            self.publish("info/CLIB", data["611"])
        if "617" in data:
            self.publish("info/MAC", data["617"])
        if "706" in data:
            self.publish("info/SN", data["706"])
        block_800 = as_dict(data.get("800"), ctx="800")
        if block_800 and "100" in block_800:
            self.devices.solar_log_model = int(block_800["100"])  # type: ignore[arg-type]
            self.publish("info/Model", str(self.devices.solar_log_model))
            logging.info("Detected Solar Log model: %s", self.devices.solar_log_model)
        if block_800 and "160" in block_800:
            self.publish("info/InstDate", block_800["160"])  # type: ignore[index]

        block_801 = as_dict(data.get("801"), ctx="801")
        if block_801 and "101" in block_801:
            self.publish("info/FW", block_801["101"])  # type: ignore[index]
        if block_801 and "102" in block_801:
            self.publish("info/FWrelD", block_801["102"])  # type: ignore[index]

    async def _process_sd_card_info(self, data: dict[str, Any]) -> None:
        """Publish SD card information if present in startup payload."""
        if "895" in data:
            sdinfo = data["895"]
            if isinstance(sdinfo, dict):
                sd_formatted = (
                    f"[{sdinfo.get(101, '')}|{sdinfo.get(103, '')}|{sdinfo.get(102, '')}|{sdinfo.get(100, '')}]"
                    f" - {sdinfo.get(104, '')}/{sdinfo.get(105, '')}"
                )
                self.publish("info/SD", sd_formatted)

    async def _process_setpoint_data(
        self, data_152: Any, data_161: Any, data_162: Any
    ) -> None:
        """Compute and publish yearly, monthly, and daily setpoints."""
        try:
            logging.debug(
                "Setpoint data - 152: %s, 161: %s, 162: %s",
                data_152,
                data_161,
                data_162,
            )

            efficiency = data_162 if data_162 else 0
            power = data_161 if data_161 else 0
            setpoint_year = efficiency * (power / 1000)

            logging.info(
                "Calculated yearly setpoint: %s (efficiency: %s, power: %s)",
                setpoint_year,
                efficiency,
                power,
            )
            self.publish("forecast/setpointYear", int(setpoint_year))

            if isinstance(data_152, list) and len(data_152) >= 12:
                current_month = datetime.now().month

                for i in range(12):
                    month = f"{i+1:02d}"
                    if i < len(data_152):
                        monthly_setpoint = (data_152[i] / 100) * setpoint_year
                        self.publish(
                            f"forecast/setpointMonth/{month}", int(monthly_setpoint)
                        )

                if current_month - 1 < len(data_152):
                    current_month_setpoint = (
                        data_152[current_month - 1] / 100
                    ) * setpoint_year
                    self.publish(
                        "forecast/setpointCurrMonth", int(current_month_setpoint)
                    )

                    daily_setpoint = (
                        current_month_setpoint / DEFAULT_SETPOINT_DAILY_DIVISOR
                    )
                    self.publish("forecast/setpointToday", int(daily_setpoint))
            else:
                logging.warning("Invalid setpoint data format: %s", data_152)

        except Exception:
            logging.exception("Setpoint data processing error")

    # -------- Fast-poll processing --------

    async def process_inverter_status(self, data: dict[str, Any]) -> None:
        """Publish per-inverter status and PAC from fast poll tables 608/782."""
        inv_names = self.devices.inverter_names
        classes = self.devices.device_classes
        if "608" in data and "782" in data and inv_names:
            status_data = data["608"]
            pac_data = data["782"]
            logging.debug("Inverter names: %s", inv_names)
            logging.debug("Status data: %s", status_data)
            logging.debug("PAC data: %s", pac_data)

            for idx in range(len(inv_names)):
                if idx < len(classes) and classes[idx] != "Batterie":
                    inverter_name = inv_names[idx]
                    status = safe_get(status_data, idx, "Unknown")
                    self.publish(f"INV/{inverter_name}/status", status)
                    pac_value = safe_get(pac_data, idx, 0)
                    pac = int(pac_value) if pac_value else 0
                    self.publish(f"INV/{inverter_name}/PAC", pac)

    async def process_inverter_extras(self, data: dict[str, Any]) -> None:
        """Publish optional per-inverter extras if present (e.g., UAC/UDC arrays)."""
        inv_names = self.devices.inverter_names
        if not inv_names:
            return
        extras: list[tuple[str, str]] = [
            ("784", "UAC"),
            ("785", "UDC"),
        ]
        for key, suffix in extras:
            if key in data:
                arr = data.get(key)
                logging.debug(
                    "Per-inverter extra '%s' present; publishing as %s", key, suffix
                )
                for idx, name in enumerate(inv_names):
                    try:
                        raw = safe_get(arr, idx)
                        val = int(raw) if raw is not None else 0
                        self.publish(f"INV/{name}/{suffix}", val)
                    except Exception:
                        continue

    async def process_switch_group_states(self, data: dict[str, Any]) -> None:
        """Publish switch group state from 801/175."""
        sg_names = self.devices.switch_group_names
        block_801 = data.get("801") if isinstance(data, dict) else None
        if isinstance(block_801, dict) and "175" in block_801 and sg_names:
            sg_data = block_801["175"]
            logging.debug("Switch group data: %s", sg_data)
            for sgsj in range(min(MAX_SWITCH_GROUPS, len(sg_names))):
                sg_name = sg_names[sgsj]
                if not sg_name:
                    continue
                try:
                    if sgsj < len(sg_data) and sg_data[sgsj]:
                        entry = sg_data[sgsj]
                        sg_state = None
                        if isinstance(entry, dict):
                            sg_state = entry.get("101", entry.get(101))
                        if sg_state is not None:
                            self.publish(f"SwitchGroup/{sg_name}/state", sg_state)
                except (KeyError, TypeError, IndexError) as e:
                    logging.debug("Error processing switch group %s: %s", sg_name, e)

    async def process_battery_data(self, data: dict[str, Any]) -> list[int]:
        """Return battery data array and publish selected metrics by inverter."""
        is_battery_present = self.devices.battery_present
        is_battery_device_present = self.devices.battery_device_present
        batt_index = self.devices.battery_index
        inv_names = self.devices.inverter_names

        battery_data = [0, 0, 0, 0]
        if "858" in data:
            battery_data = data["858"] if data["858"] else [0, 0, 0, 0]
            if len(battery_data) < 4:
                battery_data.extend([0] * (4 - len(battery_data)))
            if is_battery_present and is_battery_device_present and batt_index:
                battery_inv_idx = batt_index[0]
                if battery_inv_idx < len(inv_names):
                    battery_inv_name = inv_names[battery_inv_idx]
                    self.publish(f"INV/{battery_inv_name}/BattLevel", battery_data[1])
                    self.publish(f"INV/{battery_inv_name}/ChargePower", battery_data[2])
                    self.publish(
                        f"INV/{battery_inv_name}/DischargePower", battery_data[3]
                    )
        return battery_data

    async def process_production_consumption(
        self, data: dict[str, Any], battery_data: list[int]
    ) -> None:
        """Publish net production/consumption and feed in/out from 780/781."""
        if "780" in data and "781" in data:
            production = data["780"] or 0
            consumption = data["781"] or 0
            net_production = production - battery_data[3]
            net_consumption = consumption - battery_data[2]
            self.publish("status/pac", int(net_production))
            self.publish("status/conspac", int(net_consumption))
            feed = production - consumption
            self.publish("status/feed", int(feed))
            if feed > 0:
                self.publish("status/feedin", int(feed))
                self.publish("status/feedinactive", True)
                self.publish("status/feedout", 0)
            else:
                self.publish("status/feedin", 0)
                self.publish("status/feedinactive", False)
                self.publish("status/feedout", int(abs(feed)))

    async def process_display_data(self, display_data: list[Any]) -> None:
        """Publish overall display OK and selected elements from 794/0."""
        try:
            if not isinstance(display_data, list) or len(display_data) < 16:
                logging.warning("Invalid display data format: %s", display_data)
                return
            check_ok: list[Any] = []
            for di in range(min(16, len(display_data))):
                if len(display_data[di]) > 1:
                    check_ok.append(display_data[di][1])
                else:
                    check_ok.append(True)
            display_ok = all((not errval) for errval in check_ok)
            self.publish("display/OK", display_ok)
            display_elements = [
                (0, "invicon", "inverror", "Inverter"),
                (1, "networkicon", "networkerror", "Network"),
                (6, "metericon", "metersoffline", "Meter"),
                (11, "mailicon", "mailerror", "Mail"),
            ]
            for idx, icon_name, error_name, _desc in display_elements:
                if idx < len(display_data) and len(display_data[idx]) >= 2:
                    icon_value = display_data[idx][0]
                    error_value = display_data[idx][1]
                    self.publish(f"display/{icon_name}", icon_value)
                    self.publish(f"display/{error_name}", error_value)
        except Exception:
            logging.exception("Display data processing error")

    async def process_fast_poll(self, data: dict[str, Any]) -> None:
        """Orchestrate processing of fast-poll payloads."""
        if '608' in data and data['608']:
            first_status = None
            if isinstance(data['608'], list) and len(data['608']) > 0:
                first_status = data['608'][0]
            elif isinstance(data['608'], dict):
                first_status = data['608'].get('0', data['608'].get(0, ''))
            if first_status is not None and "DENIED" in str(first_status):
                raise AccessDeniedError("Solar Log access denied")
        logging.debug("Fast poll data keys: %s", list(data.keys()))
        known_fast = {"608", "780", "781", "782", "794", "801", "858"}
        extra_fast = {str(k) for k in data.keys()} - known_fast
        if extra_fast:
            logging.debug(
                "Fast poll contains extra keys not handled: %s", sorted(extra_fast)
            )
        await self.process_inverter_status(data)
        await self.process_inverter_extras(data)
        await self.process_switch_group_states(data)
        block_794 = data.get("794") if isinstance(data, dict) else None
        if isinstance(block_794, dict) and "0" in block_794:
            await self.process_display_data(block_794["0"])
        battery_data = await self.process_battery_data(data)
        await self.process_production_consumption(data, battery_data)

    async def process_switch_group_details(
        self, sg_data: list[Any] | dict[int, Any]
    ) -> None:
        """Process detailed switch group data (447) and publish metadata."""
        try:
            sg_names = self.devices.switch_group_names
            if not sg_names:
                return
            for sgj in range(min(MAX_SWITCH_GROUPS, len(sg_names))):
                sg_name = sg_names[sgj]
                if sg_name and sgj < len(sg_data) and sg_data[sgj]:
                    try:
                        if isinstance(sg_data[sgj], dict):
                            mode = sg_data[sgj].get("102", sg_data[sgj].get(102))
                        else:
                            mode = None
                        self.publish(f"SwitchGroup/{sg_name}/mode", mode)

                        linked_list = None
                        if isinstance(sg_data[sgj], dict):
                            linked_list = sg_data[sgj].get("101", sg_data[sgj].get(101))
                        if isinstance(linked_list, list) and len(linked_list) > 0:
                            linked_device_data = linked_list[0]
                            if self.devices.inverter_names:
                                device_idx = linked_device_data.get(
                                    "100", linked_device_data.get(100)
                                )
                                if isinstance(device_idx, int) and device_idx < len(
                                    self.devices.inverter_names
                                ):
                                    linked_device_name = self.devices.inverter_names[device_idx]
                                    self.publish(
                                        f"SwitchGroup/{sg_name}/linkeddev",
                                        linked_device_name,
                                    )
                            subunit = linked_device_data.get(
                                "101", linked_device_data.get(101)
                            )
                            self.publish(f"SwitchGroup/{sg_name}/linkeddevsub", subunit)
                    except (KeyError, TypeError, IndexError) as e:
                        logging.debug(
                            "Error processing switch group %s: %s", sg_name, e
                        )
        except Exception:
            logging.exception("Switch group detail processing error")

    # -------- Periodic-poll processing --------

    async def process_inverter_day_sums(self, data_suz: list[Any]) -> None:
        """Publish per-inverter day sums from 777/0."""
        try:
            inv_names = self.devices.inverter_names
            classes = self.devices.device_classes
            if not inv_names or not classes:
                logging.warning(
                    "Inverter names or device classes not available for day sum processing"
                )
                return
            today = datetime.now().strftime("%d.%m.%y")
            index_suz = None
            for i in range(min(DAYS_TO_CHECK_HISTORY, len(data_suz))):
                if len(data_suz[i]) > 0:
                    date_str = str(data_suz[i][0])
                    if today in date_str:
                        index_suz = i
                        break
            if index_suz is None:
                logging.warning(
                    "Could not find today's date (%s) in day sum data", today
                )
                return
            if len(data_suz[index_suz]) < 2:
                logging.warning("Day sum data structure incomplete")
                return
            daysum_data = data_suz[index_suz][1]
            nam_length = min(
                len(inv_names), len(classes), len(daysum_data) if daysum_data else 0
            )
            for suzi in range(nam_length):
                if suzi < len(classes) and classes[suzi] != "Batterie":
                    inverter_name = inv_names[suzi]
                    daysum_value = daysum_data[suzi] if suzi < len(daysum_data) else 0
                    self.publish(
                        f"INV/{inverter_name}/daysum",
                        int(daysum_value) if daysum_value else 0,
                    )
            logging.info("Processed day sums for %s inverters", nam_length)
        except Exception:
            logging.exception("Inverter day sums processing error")

    async def process_self_consumption(self, data_selfcons: list[Any]) -> None:
        """Publish daily/yesterday self-consumption metrics and ratios from 778/0."""
        try:
            today = datetime.now().strftime("%d.%m.%y")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%y")
            today_index = None
            yesterday_index = None
            for i in range(min(DAYS_TO_CHECK_HISTORY, len(data_selfcons))):
                if len(data_selfcons[i]) > 0:
                    date_str = str(data_selfcons[i][0])
                    if today in date_str:
                        today_index = i
                    if yesterday in date_str:
                        yesterday_index = i
            if today_index is not None and len(data_selfcons[today_index]) > 1:
                entry_today = data_selfcons[today_index]
                selfcons_today = (
                    entry_today[1] if len(entry_today) > 1 and entry_today[1] else 0
                )
                self.publish("SelfCons/selfconstoday", int(selfcons_today))
                dayratio = self._selfcons_ratio(selfcons_today, self.last_yield_day)
                self.publish("SelfCons/selfconsratiotoday", dayratio)
                self.last_selfcons_yesterday_fallback = selfcons_today
                self.last_selfcons_ratio_yesterday_fallback = dayratio
                if (self.devices.battery_device_present or self.devices.battery_present) and len(
                    entry_today
                ) >= 5:
                    if self.devices.battery_device_present and self.devices.battery_index:
                        battery_inv_name = self.devices.inverter_names[self.devices.battery_index[0]]
                        self.publish(
                            f"INV/{battery_inv_name}/BattSelfCons", int(entry_today[2])
                        )
                        self.publish(
                            f"INV/{battery_inv_name}/BattChargeDaysum",
                            int(entry_today[3]),
                        )
                        self.publish(
                            f"INV/{battery_inv_name}/BattDischargeDaysum",
                            int(entry_today[4]),
                        )
                    else:
                        self.publish("INV/Battery/BattSelfCons", int(entry_today[2]))
                        self.publish(
                            "INV/Battery/BattChargeDaysum", int(entry_today[3])
                        )
                        self.publish(
                            "INV/Battery/BattDischargeDaysum", int(entry_today[4])
                        )
            if yesterday_index is not None and len(data_selfcons[yesterday_index]) > 1:
                entry_yesterday = data_selfcons[yesterday_index]
                selfcons_yesterday = (
                    entry_yesterday[1]
                    if len(entry_yesterday) > 1 and entry_yesterday[1]
                    else 0
                )
                self.publish("SelfCons/selfconsyesterday", int(selfcons_yesterday))
                dayratio_y = self._selfcons_ratio(selfcons_yesterday, self.last_yield_yesterday)
                self.publish("SelfCons/selfconsratioyesterday", dayratio_y)
            else:
                self.publish(
                    "SelfCons/selfconsyesterday",
                    int(self.last_selfcons_yesterday_fallback),
                )
                self.publish(
                    "SelfCons/selfconsratioyesterday",
                    self.last_selfcons_ratio_yesterday_fallback,
                )
        except Exception:
            logging.exception("Self-consumption data processing error")

    async def process_periodic_poll(self, data: dict[str, Any]) -> None:
        """Process periodic poll payload (777/778/801/170 + switch groups elsewhere)."""
        logging.debug("Polling data keys: %s", list(data.keys()))
        if "777" in data and "0" in data["777"] and self.devices.inverter_names:
            await self.process_inverter_day_sums(data["777"]["0"])
        if "778" in data and "0" in data["778"]:
            await self.process_self_consumption(data["778"]["0"])
        block_801 = data.get("801", data.get(801)) if isinstance(data, dict) else None
        json_data = None
        if isinstance(block_801, dict):
            json_data = block_801.get("170", block_801.get(170))
        if json_data is not None:

            # Diagnostic: unknown keys in 801/170
            if isinstance(json_data, dict):
                present_keys = set()
                for k in json_data.keys():
                    try:
                        present_keys.add(int(k))
                    except Exception:
                        continue
                expected = set(range(100, 117))
                extra = present_keys - expected
                missing = expected - present_keys
                if extra:
                    logging.debug(
                        "801/170 contains extra keys not handled: %s", sorted(extra)
                    )
                if missing:
                    logging.debug("801/170 missing expected keys: %s", sorted(missing))

            pac_val = int(safe_get(json_data, 101, 0))
            pdc_val = int(safe_get(json_data, 102, 0))
            uac_val = int(safe_get(json_data, 103, 0))
            udc_val = int(safe_get(json_data, 104, 0))
            logging.debug(
                "801/170 periodic: pac=%s pdc=%s uac=%s udc=%s",
                pac_val,
                pdc_val,
                uac_val,
                udc_val,
            )
            # Do not overwrite fast-poll values with zeros; publish only if > 0
            if pac_val > 0:
                self.publish("status/pac", pac_val)
            if pdc_val > 0:
                self.publish("status/pdc", pdc_val)
            if uac_val > 0:
                self.publish("status/uac", uac_val)
            if udc_val > 0:
                self.publish("status/udc", udc_val)
            self.publish("status/conspac", int(safe_get(json_data, 110, 0)))
            self.last_yield_day = int(safe_get(json_data, 105, 0))
            self.last_yield_yesterday = int(safe_get(json_data, 106, 0))
            self.publish("status/yieldday", self.last_yield_day)
            self.publish("status/yieldyesterday", self.last_yield_yesterday)
            self.publish("status/yieldmonth", int(safe_get(json_data, 107, 0)))
            self.publish("status/yieldyear", int(safe_get(json_data, 108, 0)))
            self.publish("status/yieldtotal", int(safe_get(json_data, 109, 0)))
            self.publish("status/consyieldday", int(safe_get(json_data, 111, 0)))
            self.publish("status/consyieldyesterday", int(safe_get(json_data, 112, 0)))
            self.publish("status/consyieldmonth", int(safe_get(json_data, 113, 0)))
            self.publish("status/consyieldyear", int(safe_get(json_data, 114, 0)))
            self.publish("status/consyieldtotal", int(safe_get(json_data, 115, 0)))
            self.publish("info/lastSync", str(safe_get(json_data, 100, "")))
            # Track total power for forecast helper
            try:
                self.total_power_w = int(safe_get(json_data, 116, 0))
            except Exception:
                self.total_power_w = 0
            self.publish("info/totalPower", self.total_power_w)
        # Dispatch 447 switch group details if present
        if "447" in data:
            await self.process_switch_group_details(data["447"])
