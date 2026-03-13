"""Historic data processing for monthly/yearly Solar-Log data (Python 3.12)."""

from __future__ import annotations

from typing import Any
import logging

from .api_validation import PublishFn
from .device_registry import DeviceRegistry


class HistoricProcessor:
    """Processes 854/877/878 historic payloads and months/years JSON files."""

    def __init__(self, publish_fn: PublishFn, devices: DeviceRegistry) -> None:
        self._publish = publish_fn
        self._devices = devices

    @staticmethod
    def _selfcons_ratio(
        selfcons: int | float, total: int | float, *, unit_factor: float = 1.0
    ) -> float:
        """Self-consumption as percentage with 1 decimal. Returns 0 if total <= 0."""
        if total <= 0:
            return 0
        return round(((selfcons * unit_factor) / total) * 1000) / 10

    def _publish_period_entries(self, entries: list[Any], period: str) -> None:
        """Publish yield/cons/selfcons for each entry. period is 'monthly' or 'yearly'."""
        for entry in entries:
            if len(entry) >= 4 and entry[1]:
                date_str = entry[0]
                year = date_str[-2:]
                if period == "monthly":
                    month = date_str[3:5]
                    self._publish(f"Historic/20{year}/monthly/{month}/yieldmonth", entry[1])
                    self._publish(f"Historic/20{year}/monthly/{month}/consmonth", entry[2])
                    self._publish(f"Historic/20{year}/monthly/{month}/selfconsmonth", entry[3])
                else:
                    self._publish(f"Historic/20{year}/yieldyear", entry[1])
                    self._publish(f"Historic/20{year}/consyear", entry[2])
                    self._publish(f"Historic/20{year}/selfconsyear", entry[3])

    def _publish_selfcons_pair(
        self,
        current_entry: list[Any],
        last_entry: list[Any],
        current_topic: str,
        ratio_topic: str,
        last_topic: str,
        last_ratio_topic: str,
    ) -> None:
        """Publish selfcons value + ratio for a current/last period pair."""
        if len(current_entry) >= 4:
            self._publish(current_topic, int(current_entry[3]))
            cons = current_entry[2] or 0
            if cons > 0:
                self._publish(
                    ratio_topic,
                    self._selfcons_ratio(current_entry[3], cons, unit_factor=1000.0),
                )
        if len(last_entry) >= 4:
            self._publish(last_topic, int(last_entry[3]))
            cons_last = last_entry[2] or 0
            if cons_last > 0:
                self._publish(
                    last_ratio_topic,
                    self._selfcons_ratio(last_entry[3], cons_last, unit_factor=1000.0),
                )

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
                        for inu, inverter_name in enumerate(self._devices.inverter_names):
                            if inu < len(inverter_data) and inverter_data[inu]:
                                self._publish(
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
                    self._publish_selfcons_pair(
                        data_month_tot[-1], data_month_tot[-2],
                        "SelfCons/selfconsmonth", "SelfCons/selfconsratiomonth",
                        "SelfCons/selfconslastmonth", "SelfCons/selfconsratiolastmonth",
                    )
                self._publish_period_entries(data_month_tot, "monthly")
            # 878: yearly totals and self-cons metrics
            if "878" in data:
                data_year_tot = data["878"]
                logging.debug(
                    "Processing yearly totals (878): %s entries", len(data_year_tot)
                )
                if len(data_year_tot) >= 2:
                    self._publish_selfcons_pair(
                        data_year_tot[-1], data_year_tot[-2],
                        "SelfCons/selfconsyear", "SelfCons/selfconsratioyear",
                        "SelfCons/selfconslastyear", "SelfCons/selfconsratiolastyear",
                    )
                self._publish_period_entries(data_year_tot, "yearly")
        except Exception:
            logging.exception("Historic data processing error")

    async def process_months_json(self, data: list[Any]) -> None:
        """Process /months.json payload for monthly historic and ratios."""
        try:
            logging.debug("Processing monthly JSON data: %s entries", len(data))
            self._publish_period_entries(data, "monthly")
            if len(data) >= 2:
                self._publish_selfcons_pair(
                    data[0], data[1],
                    "SelfCons/selfconsmonth", "SelfCons/selfconsratiomonth",
                    "SelfCons/selfconslastmonth", "SelfCons/selfconsratiolastmonth",
                )
        except Exception:
            logging.exception("Monthly JSON data processing error")

    async def process_years_json(self, data: list[Any]) -> None:
        """Process /years.json payload for yearly historic and ratios."""
        try:
            logging.debug("Processing yearly JSON data: %s entries", len(data))
            self._publish_period_entries(data, "yearly")
            if len(data) >= 2:
                self._publish_selfcons_pair(
                    data[0], data[1],
                    "SelfCons/selfconsyear", "SelfCons/selfconsratioyear",
                    "SelfCons/selfconslastyear", "SelfCons/selfconsratiolastyear",
                )
        except Exception:
            logging.exception("Yearly JSON data processing error")
