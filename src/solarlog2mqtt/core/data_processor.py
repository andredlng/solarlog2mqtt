"""Solar-Log data processing (Python 3.12).

Implements startup, fast/periodic polling, historic parsing, and switch-group
handling, and publishes values via the provided MQTT publisher.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING
import logging
import math
from datetime import datetime, timedelta
from .api_validation import as_dict
from .constants import (
    MAX_SWITCH_GROUPS,
    DEFAULT_SETPOINT_DAILY_DIVISOR,
    DEVICE_CLASS_LIST,
    DAYS_TO_CHECK_HISTORY,
)

if TYPE_CHECKING:  # Avoid runtime import cycle during initial skeleton stage
    from .mqtt_publisher import MQTTPublisher


class DataProcessor:
    """Handles processing of Solar-Log device data and publishing to MQTT."""

    def __init__(self, mqtt_publisher: "MQTTPublisher | None" = None) -> None:
        self.mqtt_publisher = mqtt_publisher
        # Device state
        self.num_inverters = 0
        self.inverter_names: list[str] = []
        self.device_infos: list[int] = []
        self.device_types: list[str] = []
        self.device_brands: list[str] = []
        self.device_classes: list[str] = []
        self.switch_group_names: list[str | None] = []
        self.num_switch_groups = 0
        self.battery_device_present = False
        self.battery_index: list[int] = []
        self.device_list: dict[str, Any] | None = None
        self.brand_list: dict[str, Any] | None = None
        self.solar_log_model: int | None = None
        self.battery_present = False

        # Track latest daily production figures for ratio calculations
        self.last_yield_day = 0
        self.last_yield_yesterday = 0

        # Fallback cache for yesterday self-consumption across month boundary
        self.last_selfcons_yesterday_fallback = 99
        self.last_selfcons_ratio_yesterday_fallback = 99
        # Last known total power (W) for forecast helper
        self.total_power_w: int = 0

    def publish(self, topic: str, value: str | int | float | bool) -> None:
        """Publish a value to MQTT via the configured publisher.

        This method is intentionally lenient and returns silently if no
        publisher is configured to ease testing without a broker.
        """
        if self.mqtt_publisher:
            self.mqtt_publisher.publish(topic, value)

    async def process_startup_data(self, data: dict[str, Any]) -> None:
        """Process startup data and extract device information.

        Publishes device/system metadata, SD card info, switch groups, and
        computed setpoints where available. Stores discovery metadata for
        later classification and runtime processing.
        """
        try:
            logging.debug("Startup data keys: %s", list(data.keys()))

            # Process different aspects of startup data
            await self._process_device_system_info(data)
            await self._process_sd_card_info(data)
            await self._process_device_discovery(data)
            await self._process_switch_groups(data)
            await self._process_battery_info(data)

            # Process setpoint data if available
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

    async def classify_devices(self) -> None:
        """Classify devices using discovered lists and populate device metadata.

        Determines device type, brand, and class (battery detection) from
        device_list/brand_list tables and the collected device_infos.
        """
        try:
            logging.debug(
                "Classifying devices - device_list available: %s, brand_list available: %s, device_infos len: %s",
                self.device_list is not None,
                self.brand_list is not None,
                len(self.device_infos),
            )
            if not self.device_list or not self.brand_list or not self.device_infos:
                logging.warning(
                    "Device classification data not available, using placeholders"
                )
                self.device_types = ["Unknown"] * len(self.inverter_names)
                self.device_brands = ["Unknown"] * len(self.inverter_names)
                self.device_classes = ["Wechselrichter"] * len(self.inverter_names)
                return

            device_types: list[str] = []
            device_brands: list[str] = []
            device_classes: list[str] = []
            battery_index: list[int] = []
            battery_device_present = False

            def get_from_indexed(container, idx):
                if container is None:
                    return None
                if isinstance(container, list):
                    return container[idx] if 0 <= idx < len(container) else None
                if isinstance(container, dict):
                    if idx in container:
                        return container[idx]
                    return container.get(str(idx))
                return None

            for i, (name, info_code) in enumerate(
                zip(self.inverter_names, self.device_infos)
            ):
                try:
                    logging.debug(
                        "Processing device %s (%s) with info_code %s",
                        i,
                        name,
                        info_code,
                    )

                    try:
                        info_idx = int(info_code)
                    except Exception:
                        info_idx = info_code

                    device_info = get_from_indexed(self.device_list, info_idx)
                    logging.debug("device_list[%s] = %s", info_code, device_info)

                    if device_info:
                        # Device type (index 1)
                        if (
                            isinstance(device_info, (list, tuple))
                            and len(device_info) > 1
                        ):
                            device_type = device_info[1]
                        else:
                            device_type = "Unknown"
                        device_types.append(device_type)

                        # Brand via brand_list[device_info[0]]
                        brand_idx_val = 0
                        if (
                            isinstance(device_info, (list, tuple))
                            and len(device_info) > 0
                        ):
                            brand_idx_val = device_info[0]
                        try:
                            brand_idx_int = int(brand_idx_val)
                        except Exception:
                            brand_idx_int = brand_idx_val
                        device_brand = get_from_indexed(self.brand_list, brand_idx_int)
                        if device_brand is None:
                            device_brand = "Unknown"
                        device_brands.append(device_brand)

                        # Device class from bitmask at index 5
                        dclass_val = None
                        if (
                            isinstance(device_info, (list, tuple))
                            and len(device_info) > 5
                        ):
                            try:
                                dclass_val = int(device_info[5])
                            except Exception:
                                try:
                                    dclass_val = int(str(device_info[5]).strip())
                                except Exception:
                                    dclass_val = None
                        if dclass_val and dclass_val > 0:
                            # 2^n mapping to index in DEVICE_CLASS_LIST
                            class_idx = int(math.log(dclass_val) / math.log(2))
                            device_class = (
                                DEVICE_CLASS_LIST[class_idx]
                                if 0 <= class_idx < len(DEVICE_CLASS_LIST)
                                else "Wechselrichter"
                            )
                        else:
                            device_class = "Wechselrichter"
                        device_classes.append(device_class)

                        if device_class == "Batterie":
                            battery_device_present = True
                            battery_index.append(i)
                            logging.info(
                                "Battery device detected at index %s: %s", i, name
                            )

                        logging.debug(
                            "Device %s (%s): Type=%s, Brand=%s, Class=%s",
                            i,
                            name,
                            device_type,
                            device_brand,
                            device_class,
                        )
                    else:
                        logging.debug("No device_info for index %s", info_idx)
                        device_types.append("Unknown")
                        device_brands.append("Unknown")
                        device_classes.append("Wechselrichter")

                except (IndexError, ValueError, TypeError) as e:
                    logging.debug("Error classifying device %s (%s): %s", i, name, e)
                    device_types.append("Unknown")
                    device_brands.append("Unknown")
                    device_classes.append("Wechselrichter")

            logging.info(
                "Device classification complete. Battery devices: %s (indices: %s)",
                battery_device_present,
                battery_index,
            )
            # Store results back
            self.device_types = device_types
            self.device_brands = device_brands
            self.device_classes = device_classes
            self.battery_device_present = battery_device_present
            self.battery_index = battery_index

        except Exception:
            logging.exception("Device classification failed; using defaults")
            self.device_types = ["Unknown"] * len(self.inverter_names)
            self.device_brands = ["Unknown"] * len(self.inverter_names)
            self.device_classes = ["Wechselrichter"] * len(self.inverter_names)

    async def publish_device_info(self) -> None:
        """Publish per-device metadata (class/type/brand) to MQTT."""
        try:
            inv_names = self.inverter_names
            types = self.device_types
            brands = self.device_brands
            classes = self.device_classes
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
            self.solar_log_model = int(block_800["100"])  # type: ignore[arg-type]
            self.publish("info/Model", str(self.solar_log_model))
            logging.info("Detected Solar Log model: %s", self.solar_log_model)
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

    async def _process_device_discovery(self, data: dict[str, Any]) -> None:
        """Count inverters/meters based on discovery table 740 and publish size."""
        if "739" in data:
            self.device_list = data["739"]
            logging.debug("Device list: %s", self.device_list)
        if "744" in data:
            self.brand_list = data["744"]
            logging.debug("Brand list: %s", self.brand_list)

        logging.debug("About to process '740' data...")
        try:
            if "740" in data:
                data_740 = as_dict(data.get("740"), ctx="740") or {}
                logging.debug("Device discovery data (740): %s", data_740)

                numinv = 0
                statusuz = ""
                while statusuz != "Err" and numinv < 100:
                    statusuz = data_740.get(str(numinv), "Err")  # type: ignore[assignment]
                    logging.debug("Checking device %s: status = %s", numinv, statusuz)
                    if statusuz != "Err":
                        numinv += 1
                    else:
                        break

                self.num_inverters = numinv
                logging.info("Number of inverters/meters: %s", self.num_inverters)
                # JS publishes info.numinv as (numinv - 1)
                self.publish("info/numinv", max(self.num_inverters - 1, 0))
            else:
                logging.warning("No '740' data found in startup response")
                self.num_inverters = 0

            # Requesting device info is handled by orchestrator (startup sequence).

        except Exception:
            logging.exception("Error in device discovery")
            self.num_inverters = 0

    async def _process_switch_groups(self, data: dict[str, Any]) -> None:
        """Extract switch group names and publish their count."""
        if "447" in data:
            sgdata = as_dict(data.get("447"), ctx="447") or {}
            logging.debug("Switch group data: %s", sgdata)

            self.switch_group_names = []
            for isg in range(MAX_SWITCH_GROUPS):
                try:
                    entry = sgdata.get(isg)  # type: ignore[index]
                    sg_name = entry.get(100) if isinstance(entry, dict) else None
                    if sg_name:
                        clean_name = sg_name.replace(" ", "")
                        self.switch_group_names.append(clean_name)
                        logging.debug("Found switch group: %s", clean_name)
                    else:
                        self.switch_group_names.append(None)
                except Exception:
                    self.switch_group_names.append(None)

            self.num_switch_groups = len(
                [name for name in self.switch_group_names if name]
            )
            logging.info("Number of switch groups: %s", self.num_switch_groups)

    async def _process_battery_info(self, data: dict[str, Any]) -> None:
        """Detect presence of a battery device from startup payload."""
        if "858" in data and data["858"]:
            self.battery_present = len(data["858"]) > 0
            logging.info("Battery detected: %s", self.battery_present)

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

            # Process monthly setpoints from data_152 array
            if isinstance(data_152, list) and len(data_152) >= 12:
                current_month = datetime.now().month

                month_names = [
                    "01",
                    "02",
                    "03",
                    "04",
                    "05",
                    "06",
                    "07",
                    "08",
                    "09",
                    "10",
                    "11",
                    "12",
                ]
                for i, month in enumerate(month_names):
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
        inv_names = self.inverter_names
        classes = self.device_classes
        if "608" in data and "782" in data and inv_names:
            status_data = data["608"]
            pac_data = data["782"]
            logging.debug("Inverter names: %s", inv_names)
            logging.debug("Status data: %s", status_data)
            logging.debug("PAC data: %s", pac_data)

            for idx in range(len(inv_names)):
                if idx < len(classes) and classes[idx] != "Batterie":
                    inverter_name = inv_names[idx]
                    # Status
                    if isinstance(status_data, list):
                        status = (
                            status_data[idx] if idx < len(status_data) else "Unknown"
                        )
                    else:
                        status = status_data.get(
                            str(idx), status_data.get(idx, "Unknown")
                        )
                    self.publish(f"INV/{inverter_name}/status", status)
                    # PAC
                    if isinstance(pac_data, list):
                        pac_value = pac_data[idx] if idx < len(pac_data) else 0
                    else:
                        pac_value = pac_data.get(str(idx), pac_data.get(idx, 0))
                    pac = int(pac_value) if pac_value else 0
                    self.publish(f"INV/{inverter_name}/PAC", pac)

    async def process_inverter_extras(self, data: dict[str, Any]) -> None:
        """Publish optional per-inverter extras if present (e.g., UAC/UDC arrays)."""
        inv_names = self.inverter_names
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
                        val = 0
                        if isinstance(arr, list):
                            val = (
                                int(arr[idx])
                                if idx < len(arr) and arr[idx] is not None
                                else 0
                            )
                        elif isinstance(arr, dict):
                            raw = arr.get(idx, arr.get(str(idx)))
                            val = int(raw) if raw is not None else 0
                        self.publish(f"INV/{name}/{suffix}", val)
                    except Exception:
                        continue

    async def process_switch_group_states(self, data: dict[str, Any]) -> None:
        """Publish switch group state from 801/175."""
        sg_names = self.switch_group_names
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
        is_battery_present = self.battery_present
        is_battery_device_present = self.battery_device_present
        batt_index = self.battery_index
        inv_names = self.inverter_names

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
        logging.debug("Fast poll data keys: %s", list(data.keys()))
        # Diagnostic: unknown fast-poll keys
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
            sg_names = self.switch_group_names
            if not sg_names:
                return
            for sgj in range(min(MAX_SWITCH_GROUPS, len(sg_names))):
                sg_name = sg_names[sgj]
                if sg_name and sgj < len(sg_data) and sg_data[sgj]:
                    try:
                        # Switch group mode (102)
                        if isinstance(sg_data[sgj], dict):
                            mode = sg_data[sgj].get("102", sg_data[sgj].get(102))
                        else:
                            mode = None
                        self.publish(f"SwitchGroup/{sg_name}/mode", mode)

                        # Linked device info (101.0.100 and 101.0.101)
                        linked_list = None
                        if isinstance(sg_data[sgj], dict):
                            linked_list = sg_data[sgj].get("101", sg_data[sgj].get(101))
                        if isinstance(linked_list, list) and len(linked_list) > 0:
                            linked_device_data = linked_list[0]
                            if self.inverter_names:
                                device_idx = linked_device_data.get(
                                    "100", linked_device_data.get(100)
                                )
                                if isinstance(device_idx, int) and device_idx < len(
                                    self.inverter_names
                                ):
                                    linked_device_name = self.inverter_names[device_idx]
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
            inv_names = self.inverter_names
            classes = self.device_classes
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
                if self.last_yield_day > 0:
                    dayratio = round((selfcons_today / self.last_yield_day) * 1000) / 10
                else:
                    dayratio = 0
                self.publish("SelfCons/selfconsratiotoday", dayratio)
                self.last_selfcons_yesterday_fallback = selfcons_today
                self.last_selfcons_ratio_yesterday_fallback = dayratio
                if (self.battery_device_present or self.battery_present) and len(
                    entry_today
                ) >= 5:
                    if self.battery_device_present and self.battery_index:
                        battery_inv_name = self.inverter_names[self.battery_index[0]]
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
                if self.last_yield_yesterday > 0:
                    dayratio_y = (
                        round((selfcons_yesterday / self.last_yield_yesterday) * 1000)
                        / 10
                    )
                else:
                    dayratio_y = 0
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
        if "777" in data and "0" in data["777"] and self.inverter_names:
            await self.process_inverter_day_sums(data["777"]["0"])
        if "778" in data and "0" in data["778"]:
            await self.process_self_consumption(data["778"]["0"])
        block_801 = data.get("801", data.get(801)) if isinstance(data, dict) else None
        json_data = None
        if isinstance(block_801, dict):
            json_data = block_801.get("170", block_801.get(170))
        if json_data is not None:

            # Helper to read numeric or string keys transparently
            def jget(k: int, default: int | str = 0):
                if isinstance(json_data, dict):
                    return json_data.get(k, json_data.get(str(k), default))
                if isinstance(json_data, (list, tuple)):
                    try:
                        return json_data[k]
                    except Exception:
                        return default
                return default

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

            pac_val = int(jget(101, 0))
            pdc_val = int(jget(102, 0))
            uac_val = int(jget(103, 0))
            udc_val = int(jget(104, 0))
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
            self.publish("status/conspac", int(jget(110, 0)))
            self.last_yield_day = int(jget(105, 0))
            self.last_yield_yesterday = int(jget(106, 0))
            self.publish("status/yieldday", self.last_yield_day)
            self.publish("status/yieldyesterday", self.last_yield_yesterday)
            self.publish("status/yieldmonth", int(jget(107, 0)))
            self.publish("status/yieldyear", int(jget(108, 0)))
            self.publish("status/yieldtotal", int(jget(109, 0)))
            self.publish("status/consyieldday", int(jget(111, 0)))
            self.publish("status/consyieldyesterday", int(jget(112, 0)))
            self.publish("status/consyieldmonth", int(jget(113, 0)))
            self.publish("status/consyieldyear", int(jget(114, 0)))
            self.publish("status/consyieldtotal", int(jget(115, 0)))
            self.publish("info/lastSync", str(jget(100, "")))
            # Track total power for forecast helper
            try:
                self.total_power_w = int(jget(116, 0))
            except Exception:
                self.total_power_w = 0

    # -------- Historic parsing --------

    async def process_historic_response(
        self, req_data: str, data: dict[str, Any]
    ) -> None:
        """Process 854/877/878 historic payloads from /getjp API."""
        try:
            logging.debug("Historic data response keys: %s", list(data.keys()))
            # 854: per-inverter yearly data
            if "854" in data:
                data_year = data["854"]
                logging.debug(
                    "Processing yearly data (854): %s entries", len(data_year)
                )
                for entry in data_year:
                    if len(entry) >= 2 and entry[1]:
                        year = entry[0][-2:]
                        inverter_data = entry[1]
                        for inu, inverter_name in enumerate(self.inverter_names):
                            if inu < len(inverter_data) and inverter_data[inu]:
                                self.publish(
                                    f"Historic/20{year}/yieldyearINV/{inverter_name}",
                                    inverter_data[inu],
                                )
            # 877: monthly totals and self-cons metrics
            if "877" in data:
                data_month_tot = data["877"]
                logging.debug(
                    "Processing monthly totals (877): %s entries", len(data_month_tot)
                )
                if len(data_month_tot) >= 2:
                    current_month = data_month_tot[-1]
                    if len(current_month) >= 4:
                        self.publish("SelfCons/selfconsmonth", int(current_month[3]))
                        cons = current_month[2] or 0
                        if cons > 0:
                            monthly_ratio = (
                                round(((current_month[3] * 1000) / cons) * 1000) / 10
                            )
                            self.publish("SelfCons/selfconsratiomonth", monthly_ratio)
                    if len(data_month_tot) >= 2:
                        last_month = data_month_tot[-2]
                        if len(last_month) >= 4:
                            self.publish(
                                "SelfCons/selfconslastmonth", int(last_month[3])
                            )
                            cons_last = last_month[2] or 0
                            if cons_last > 0:
                                last_monthly_ratio = (
                                    round(((last_month[3] * 1000) / cons_last) * 1000)
                                    / 10
                                )
                                self.publish(
                                    "SelfCons/selfconsratiolastmonth",
                                    last_monthly_ratio,
                                )
                for entry in data_month_tot:
                    if len(entry) >= 4 and entry[1]:
                        date_str = entry[0]
                        year = date_str[-2:]
                        month = date_str[3:5]
                        self.publish(
                            f"Historic/20{year}/monthly/{month}/yieldmonth", entry[1]
                        )
                        self.publish(
                            f"Historic/20{year}/monthly/{month}/consmonth", entry[2]
                        )
                        self.publish(
                            f"Historic/20{year}/monthly/{month}/selfconsmonth", entry[3]
                        )
            # 878: yearly totals and self-cons metrics
            if "878" in data:
                data_year_tot = data["878"]
                logging.debug(
                    "Processing yearly totals (878): %s entries", len(data_year_tot)
                )
                if len(data_year_tot) >= 2:
                    current_year = data_year_tot[-1]
                    if len(current_year) >= 4:
                        self.publish("SelfCons/selfconsyear", int(current_year[3]))
                        cons_y = current_year[2] or 0
                        if cons_y > 0:
                            yearly_ratio = (
                                round(((current_year[3] * 1000) / cons_y) * 1000) / 10
                            )
                            self.publish("SelfCons/selfconsratioyear", yearly_ratio)
                    if len(data_year_tot) >= 2:
                        last_year = data_year_tot[-2]
                        if len(last_year) >= 4:
                            self.publish("SelfCons/selfconslastyear", int(last_year[3]))
                            cons_yl = last_year[2] or 0
                            if cons_yl > 0:
                                last_yearly_ratio = (
                                    round(((last_year[3] * 1000) / cons_yl) * 1000) / 10
                                )
                                self.publish(
                                    "SelfCons/selfconsratiolastyear", last_yearly_ratio
                                )
                for entry in data_year_tot:
                    if len(entry) >= 4 and entry[1]:
                        year = entry[0][-2:]
                        self.publish(f"Historic/20{year}/yieldyear", entry[1])
                        self.publish(f"Historic/20{year}/consyear", entry[2])
                        self.publish(f"Historic/20{year}/selfconsyear", entry[3])
        except Exception:
            logging.exception("Historic data processing error")

    async def process_months_json(self, data: list[Any]) -> None:
        """Process /months.json payload for monthly historic and ratios."""
        try:
            logging.debug("Processing monthly JSON data: %s entries", len(data))
            for entry in data:
                if len(entry) >= 4:
                    date_str = entry[0]
                    year = date_str[-2:]
                    month = date_str[3:5]
                    if entry[1]:
                        self.publish(
                            f"Historic/20{year}/monthly/{month}/yieldmonth", entry[1]
                        )
                        self.publish(
                            f"Historic/20{year}/monthly/{month}/consmonth", entry[2]
                        )
                        self.publish(
                            f"Historic/20{year}/monthly/{month}/selfconsmonth", entry[3]
                        )
            if len(data) >= 2:
                current_month_json = data[0]
                if len(current_month_json) >= 4:
                    selfcons_month = int(current_month_json[3])
                    self.publish("SelfCons/selfconsmonth", selfcons_month)
                    cons = current_month_json[2] or 0
                    if cons > 0:
                        ratio_month = (
                            round(((current_month_json[3] * 1000) / cons) * 1000) / 10
                        )
                        self.publish("SelfCons/selfconsratiomonth", ratio_month)
                if len(data) > 1:
                    last_month_json = data[1]
                    if len(last_month_json) >= 4:
                        selfcons_lastmonth = int(last_month_json[3])
                        self.publish("SelfCons/selfconslastmonth", selfcons_lastmonth)
                        cons_last = last_month_json[2] or 0
                        if cons_last > 0:
                            ratio_lastmonth = (
                                round(((last_month_json[3] * 1000) / cons_last) * 1000)
                                / 10
                            )
                            self.publish(
                                "SelfCons/selfconsratiolastmonth", ratio_lastmonth
                            )
        except Exception:
            logging.exception("Monthly JSON data processing error")

    async def process_years_json(self, data: list[Any]) -> None:
        """Process /years.json payload for yearly historic and ratios."""
        try:
            logging.debug("Processing yearly JSON data: %s entries", len(data))
            for entry in data:
                if len(entry) >= 4:
                    date_str = entry[0]
                    year = date_str[-2:]
                    if entry[1]:
                        self.publish(f"Historic/20{year}/yieldyear", entry[1])
                        self.publish(f"Historic/20{year}/consyear", entry[2])
                        self.publish(f"Historic/20{year}/selfconsyear", entry[3])
            if len(data) >= 2:
                current_year_json = data[0]
                if len(current_year_json) >= 4:
                    selfcons_year = int(current_year_json[3])
                    self.publish("SelfCons/selfconsyear", selfcons_year)
                    cons_y = current_year_json[2] or 0
                    if cons_y > 0:
                        ratio_year = (
                            round(((current_year_json[3] * 1000) / cons_y) * 1000) / 10
                        )
                        self.publish("SelfCons/selfconsratioyear", ratio_year)
                if len(data) > 1:
                    last_year_json = data[1]
                    if len(last_year_json) >= 4:
                        selfcons_lastyear = int(last_year_json[3])
                        self.publish("SelfCons/selfconslastyear", selfcons_lastyear)
                        cons_yl = last_year_json[2] or 0
                        if cons_yl > 0:
                            ratio_lastyear = (
                                round(((last_year_json[3] * 1000) / cons_yl) * 1000)
                                / 10
                            )
                            self.publish(
                                "SelfCons/selfconsratiolastyear", ratio_lastyear
                            )
        except Exception:
            logging.exception("Yearly JSON data processing error")
