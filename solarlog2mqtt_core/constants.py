"""Central constants for solarlog2mqtt (Python 3.12)."""

# Request/backoff thresholds
MAX_REQUEST_FAILURES = 4
MAX_LOGIN_FAILURES = 4

# Restart / health
DEFAULT_RESTART_DELAY = 90
DEFAULT_HEALTH_CHECK_INTERVAL = 0

# Device discovery/limits
MAX_DEVICES_DISCOVERY = 64
MAX_SWITCH_GROUPS = 10
DAYS_TO_CHECK_HISTORY = 31

# Forecast / setpoints
DEFAULT_SETPOINT_DAILY_DIVISOR = 30
FORECAST_API_RATE_LIMIT = 12

# HTTP
HTTP_TIMEOUT_SECONDS = 10

# Process exit codes
EXIT_CODE_RESTART_REQUIRED = 2

# Solar Log API endpoints and data requests
STARTUP_DATA = '{"152":null,"161":null,"162":null,"447":null,"610":null,"611":null,"617":null,"706":null,"739":null,"740":null,"744":null,"800":{"100":null,"160":null},"801":{"101":null,"102":null},"858":null,"895":{"100":null,"101":null,"102":null,"103":null,"104":null,"105":null}}'
POLLING_DATA = '{"447":null,"777":{"0":null},"778":{"0":null},"801":{"170":null}}'
FAST_POLL_DATA = '{"608":null,"780":null,"781":null,"782":null,"794":{"0":null},"801":{"175":null},"858":null}'
HISTORIC_DATA = '{"854":null,"877":null,"878":null}'

# Device classification constants
DEVICE_CLASS_LIST = [
    "Wechselrichter",  # 0 - Inverter
    "Sensor",  # 1 - Sensor
    "Zähler",  # 2 - Meter
    "Hybrid-System",  # 3 - Hybrid System
    "Batterie",  # 4 - Battery
    "Intelligente Verbraucher",  # 5 - Smart Consumer
    "Schalter",  # 6 - Switch
    "Wärmepumpe",  # 7 - Heat Pump
    "Heizstab",  # 8 - Heating Element
    "Ladestation",  # 9 - Charging Station
]
