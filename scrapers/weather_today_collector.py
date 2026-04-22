import json
import logging
import logging.handlers
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

WEATHER_TODAY_KEY = os.getenv("WEATHER_TODAY_KEY")

LIVE_DIR = PROJECT_ROOT / "data" / "live"
OUTPUT_JSON = LIVE_DIR / "weather_today.json"
LOG_FILE = PROJECT_ROOT / "logs" / "weather_today.log"

KYIV_TZ = ZoneInfo("Europe/Kyiv")
SLEEP_BETWEEN = 2

BASE_URL = (
    "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
    "/timeline/{location}/{date}"
    "?unitGroup=metric&key={key}&contentType=json&include=hours"
)

REGIONS: dict[str, tuple[str, str]] = {
    "Kyiv Oblast": ("Kyiv, Ukraine", "Kyiv"),
    "City of Kyiv": ("Kyiv, Ukraine", "Kyiv"),
    "Kharkiv Oblast": ("Kharkiv, Ukraine", "Kharkiv"),
    "Odesa Oblast": ("Odesa, Ukraine", "Odesa"),
    "Lviv Oblast": ("Lviv, Ukraine", "Lviv"),
    "Vinnytsia Oblast": ("Vinnytsia, Ukraine", "Vinnytsia"),
    "Dnipropetrovsk Oblast": ("Dnipro, Ukraine", "Dnipro"),
    "Donetsk Oblast": ("Pokrovsk, Ukraine", "Donetsk"),
    "Zhytomyr Oblast": ("Zhytomyr, Ukraine", "Zhytomyr"),
    "Zaporizhzhia Oblast": ("Zaporizhzhia, Ukraine", "Zaporozhye"),
    "Ivano-Frankivsk Oblast": ("Ivano-Frankivsk, Ukraine", "Ivano-Frankivsk"),
    "Kirovohrad Oblast": ("Kropyvnytskyi, Ukraine", "Kropyvnytskyi"),
    "Volyn Oblast": ("Lutsk, Ukraine", "Lutsk"),
    "Mykolaiv Oblast": ("Mykolaiv, Ukraine", "Mykolaiv"),
    "Poltava Oblast": ("Poltava, Ukraine", "Poltava"),
    "Rivne Oblast": ("Rivne, Ukraine", "Rivne"),
    "Sumy Oblast": ("Sumy, Ukraine", "Sumy"),
    "Ternopil Oblast": ("Ternopil, Ukraine", "Ternopil"),
    "Zakarpattia Oblast": ("Uzhhorod, Ukraine", "Uzhgorod"),
    "Kherson Oblast": ("Kherson, Ukraine", "Kherson"),
    "Khmelnytskyi Oblast": ("Khmelnytskyi, Ukraine", "Khmelnytskyi"),
    "Cherkasy Oblast": ("Cherkasy, Ukraine", "Cherkasy"),
    "Chernivtsi Oblast": ("Chernivtsi, Ukraine", "Chernivtsi"),
    "Chernihiv Oblast": ("Chernihiv, Ukraine", "Chernihiv"),
    "Luhansk Oblast": ("Starobilsk, Ukraine", "Luhansk"),
}

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("weather_today")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=1 * 1024 * 1024, backupCount=2, encoding="utf-8"
        )
        fh.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger

def _fetch(location: str, date_str: str,
           logger: logging.Logger) -> dict | None:
    url = BASE_URL.format(
        location=requests.utils.quote(location, safe=","),
        date=date_str,
        key=WEATHER_TODAY_KEY,
    )
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning(f"Rate limit 429 for {location}, waiting {wait}s")
                time.sleep(wait)
            elif resp.status_code == 400:
                logger.warning(f"Bad request 400 for {location} — skipping")
                return None
            else:
                logger.warning(f"{location} → HTTP {resp.status_code}, attempt {attempt + 1}/3")
                time.sleep(10)
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for {location}, attempt {attempt + 1}/3")
            time.sleep(10)
        except Exception as e:
            logger.error(f"Error for {location}: {e}")
            return None
    logger.error(f"All retries failed for {location}")
    return None

def _parse_hours(raw: dict) -> dict[str, dict]:
    hours_out: dict[str, dict] = {}
    days = raw.get("days", [])
    if not days:
        return hours_out

    for h in days[0].get("hours", []):
        time_str = h.get("datetime", "")
        if not time_str:
            continue
        try:
            dt_obj = datetime.strptime(time_str, "%H:%M:%S")
        except ValueError:
            try:
                dt_obj = datetime.strptime(time_str, "%H:%M")
            except ValueError:
                continue

        hhmm = dt_obj.strftime("%H:%M")

        hours_out[hhmm] = {
            "temp": float(h.get("temp", 0) or 0),
            "humidity": float(h.get("humidity", 0) or 0),
            "windspeed": float(h.get("windspeed", 0) or 0),
            "winddir": float(h.get("winddir", 0) or 0),
            "visibility": float(h.get("visibility", 0) or 0),
            "cloudcover": float(h.get("cloudcover", 0) or 0),
            "pressure": float(h.get("pressure", 0) or 0),
            "precip": float(h.get("precip", 0) or 0),
            "conditions": str(h.get("conditions", "")),
        }
    return hours_out

def _write_atomic(data: dict, path: Path, logger: logging.Logger) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".json", text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
        tmp = None
        logger.info(f"Saved: {path}  ({path.stat().st_size // 1024} KB)")
    except Exception as e:
        logger.error(f"Failed to write {path.name}: {e}")
        sys.exit(1)
    finally:
        if tmp and os.path.exists(tmp):
            os.remove(tmp)

def main() -> None:
    logger = setup_logging()

    if not WEATHER_TODAY_KEY:
        logger.error("WEATHER_TODAY_KEY not set in .env — add your second account key")
        sys.exit(1)

    today = datetime.now(KYIV_TZ).date()
    date_str = today.isoformat()

    logger.info("-" * 60)
    logger.info("WEATHER TODAY COLLECTOR")
    logger.info(f"Date:    {date_str}")
    logger.info(f"Regions: {len(REGIONS)}  (est. {len(REGIONS) * 24} API records)")
    logger.info("-" * 60)

    regions_out: dict[str, dict[str, dict]] = {}
    errors = 0

    for i, (region_name, (location, _city)) in enumerate(REGIONS.items(), 1):
        logger.info(f"  [{i}/{len(REGIONS)}] {region_name}")
        raw = _fetch(location, date_str, logger)
        if raw is None:
            errors += 1
            logger.error(f"  FAILED: {region_name}")
        else:
            hours = _parse_hours(raw)
            if hours:
                regions_out[region_name] = hours
                logger.info(f"  OK: {len(hours)} hours")
            else:
                errors += 1
                logger.warning(f"  No hours in response: {region_name}")

        if i < len(REGIONS):
            time.sleep(SLEEP_BETWEEN)

    if not regions_out:
        logger.error("No data collected — keeping old cache, not overwriting")
        sys.exit(1)

    _write_atomic({
        "generated_at": datetime.now(KYIV_TZ).isoformat(),
        "date": date_str,
        "regions_collected": len(regions_out),
        "errors": errors,
        "regions": regions_out,
    }, OUTPUT_JSON, logger)

    logger.info(f"Done — {len(regions_out)} regions, {errors} errors")

if __name__ == "__main__":
    main()