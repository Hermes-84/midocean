from __future__ import annotations
import os, logging
from datetime import datetime
import pytz

TZ = os.getenv("TZ", "Europe/Rome")
SUPPLIER = os.getenv("SUPPLIER_NAME", "Mid Ocean Brands")
LANG = os.getenv("MIDOCEAN_LANGUAGE", "it")

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("midocean")

def now_local():
    tz = pytz.timezone(TZ)
    return datetime.now(tz)

def today_str():
    return now_local().strftime("%Y-%m-%d")

def today_compact():
    return now_local().strftime("%Y%m%d")

def time_hms():
    return now_local().strftime("%H:%M:%S")

def to_it_decimal(val: str | float | int | None) -> str:
    if val is None or val == "" or (isinstance(val, str) and val.lower() == "null"):
        return ""
    try:
        v = float(str(val).replace(",", "."))
        s = (f"{v:.6f}").rstrip("0").rstrip(".")
        return s.replace(".", ",")
    except Exception:
        return str(val)

def to_upper(val: str | None) -> str:
    return "" if val is None else str(val).upper()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_csv(df, path: str):
    ensure_dir(os.path.dirname(path))
    df.to_csv(path, index=False)
    log.info("Wrote %s rows → %s", len(df), path)
