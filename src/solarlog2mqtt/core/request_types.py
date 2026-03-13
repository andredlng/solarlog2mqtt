"""Request type classification for Solar-Log API responses (Python 3.12)."""

from __future__ import annotations

from enum import Enum, auto


class RequestType(Enum):
    STARTUP = auto()
    DEVICE_INFO = auto()
    PERIODIC_POLL = auto()
    FAST_POLL = auto()
    HISTORIC = auto()
    MONTHS_JSON = auto()
    YEARS_JSON = auto()
    SIMPLE_POLL = auto()
    UNKNOWN = auto()


def classify_request(req_data: str) -> RequestType:
    """Classify a Solar-Log request string into a RequestType."""
    if req_data.startswith("/months.json"):
        return RequestType.MONTHS_JSON
    if req_data.startswith("/years.json"):
        return RequestType.YEARS_JSON
    if req_data.startswith('{"152"'):
        return RequestType.STARTUP
    if req_data.startswith('{"141"'):
        return RequestType.DEVICE_INFO
    if req_data.startswith('{"447"'):
        return RequestType.PERIODIC_POLL
    if req_data.startswith('{"608"'):
        return RequestType.FAST_POLL
    if req_data.startswith('{"854"'):
        return RequestType.HISTORIC
    if req_data.startswith('{"801"'):
        return RequestType.SIMPLE_POLL
    return RequestType.UNKNOWN
