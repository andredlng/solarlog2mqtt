"""Device registry: holds device state and classification logic (Python 3.12)."""

from __future__ import annotations

from typing import Any
import logging
import math

from .api_validation import as_dict, safe_get
from .constants import (
    MAX_SWITCH_GROUPS,
    MAX_DEVICES_DISCOVERY,
    DEVICE_CLASS_LIST,
)


class DeviceRegistry:
    """Owns all device-related state and discovery/classification logic."""

    def __init__(self) -> None:
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

    def _classify_single_device(
        self, i: int, name: str, info_code: int
    ) -> tuple[str, str, str, bool]:
        """Classify one device. Returns (type, brand, class, is_battery)."""
        try:
            info_idx = int(info_code)
        except Exception:
            info_idx = info_code

        device_info = safe_get(self.device_list, info_idx)

        if not device_info:
            logging.debug("No device_info for index %s", info_idx)
            return "Unknown", "Unknown", "Wechselrichter", False

        # Device type (index 1)
        device_type = (
            device_info[1]
            if isinstance(device_info, (list, tuple)) and len(device_info) > 1
            else "Unknown"
        )

        # Brand via brand_list[device_info[0]]
        brand_idx_val = (
            device_info[0]
            if isinstance(device_info, (list, tuple)) and len(device_info) > 0
            else 0
        )
        try:
            brand_idx_int = int(brand_idx_val)
        except Exception:
            brand_idx_int = brand_idx_val
        device_brand = safe_get(self.brand_list, brand_idx_int) or "Unknown"

        # Device class from bitmask at index 5
        device_class = "Wechselrichter"
        if isinstance(device_info, (list, tuple)) and len(device_info) > 5:
            try:
                dclass_val = int(device_info[5])
            except Exception:
                dclass_val = 0
            if dclass_val > 0:
                class_idx = int(math.log2(dclass_val))
                if 0 <= class_idx < len(DEVICE_CLASS_LIST):
                    device_class = DEVICE_CLASS_LIST[class_idx]

        is_battery = device_class == "Batterie"
        if is_battery:
            logging.info("Battery device detected at index %s: %s", i, name)

        logging.debug(
            "Device %s (%s): Type=%s, Brand=%s, Class=%s",
            i, name, device_type, device_brand, device_class,
        )
        return device_type, device_brand, device_class, is_battery

    async def classify_devices(self) -> None:
        """Classify devices using discovered lists and populate device metadata."""
        try:
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

            for i, (name, info_code) in enumerate(
                zip(self.inverter_names, self.device_infos)
            ):
                dtype, brand, dclass, is_batt = self._classify_single_device(i, name, info_code)
                device_types.append(dtype)
                device_brands.append(brand)
                device_classes.append(dclass)
                if is_batt:
                    battery_index.append(i)

            logging.info(
                "Device classification complete. Battery devices: %s (indices: %s)",
                bool(battery_index),
                battery_index,
            )
            self.device_types = device_types
            self.device_brands = device_brands
            self.device_classes = device_classes
            self.battery_device_present = bool(battery_index)
            self.battery_index = battery_index

        except Exception:
            logging.exception("Device classification failed; using defaults")
            self.device_types = ["Unknown"] * len(self.inverter_names)
            self.device_brands = ["Unknown"] * len(self.inverter_names)
            self.device_classes = ["Wechselrichter"] * len(self.inverter_names)

    async def process_device_discovery(self, data: dict[str, Any]) -> None:
        """Count inverters/meters based on discovery table 740 and publish size."""
        if "739" in data:
            self.device_list = data["739"]
            logging.debug("Device list: %s", self.device_list)
        if "744" in data:
            self.brand_list = data["744"]
            logging.debug("Brand list: %s", self.brand_list)

        try:
            if "740" in data:
                data_740 = as_dict(data.get("740"), ctx="740") or {}
                logging.debug("Device discovery data (740): %s", data_740)

                numinv = 0
                statusuz = ""
                while statusuz != "Err" and numinv < 100:
                    statusuz = data_740.get(str(numinv), "Err")  # type: ignore[assignment]
                    if statusuz != "Err":
                        numinv += 1
                    else:
                        break

                self.num_inverters = numinv
                logging.info("Number of inverters/meters: %s", self.num_inverters)
            else:
                logging.warning("No '740' data found in startup response")
                self.num_inverters = 0

        except Exception:
            logging.exception("Error in device discovery")
            self.num_inverters = 0

    async def process_switch_groups(self, data: dict[str, Any]) -> None:
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

    async def process_battery_info(self, data: dict[str, Any]) -> None:
        """Detect presence of a battery device from startup payload."""
        if "858" in data and data["858"]:
            self.battery_present = len(data["858"]) > 0
            logging.info("Battery detected: %s", self.battery_present)
