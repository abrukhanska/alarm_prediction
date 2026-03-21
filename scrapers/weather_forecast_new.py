import argparse
import csv
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

WEATHER_KEY   = os.getenv("my_weather_key")
RAW_NEW_DIR   = PROJECT_ROOT / "data" / "raw" / "weather" / "new"
LOG_FILE      = PROJECT_ROOT / "logs" / "weather_collector.log"

KYIV_TZ       = ZoneInfo("Europe/Kyiv")
SLEEP_BETWEEN = 2

BASE_URL = (
    "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
    "/timeline/{location}/{date}"
    "?unitGroup=metric&key={key}&contentType=json&include=hours"
)

REGIONS: dict[str, tuple[str, str]] = {
    "Kyiv_Oblast":            ("Kyiv, Ukraine",             "Kyiv"),
    "Kharkiv_Oblast":         ("Kharkiv, Ukraine",          "Kharkiv"),
    "Odesa_Oblast":           ("Odesa, Ukraine",            "Odesa"),
    "Lviv_Oblast":            ("Lviv, Ukraine",             "Lviv"),
    "Vinnytsia_Oblast":       ("Vinnytsia, Ukraine",        "Vinnytsia"),
    "Dnipropetrovska_Oblast": ("Dnipro, Ukraine",           "Dnipro"),
    "Donetsk_Oblast":         ("Pokrovsk, Ukraine",         "Donetsk"),
    "Zhytomyr_Oblast":        ("Zhytomyr, Ukraine",         "Zhytomyr"),
    "Zaporizhzhia_Oblast":    ("Zaporizhzhia, Ukraine",     "Zaporozhye"),
    "Ivano-Frankivsk_Oblast": ("Ivano-Frankivsk, Ukraine",  "Ivano-Frankivsk"),
    "Kirovohradska_Oblast":   ("Kropyvnytskyi, Ukraine",    "Kropyvnytskyi"),
    "Volyn_Oblast":           ("Lutsk, Ukraine",            "Lutsk"),
    "Mykolaiv_Oblast":        ("Mykolaiv, Ukraine",         "Mykolaiv"),
    "Poltava_Oblast":         ("Poltava, Ukraine",          "Poltava"),
    "Rivne_Oblast":           ("Rivne, Ukraine",            "Rivne"),
    "Sumy_Oblast":            ("Sumy, Ukraine",             "Sumy"),
    "Ternopil_Oblast":        ("Ternopil, Ukraine",         "Ternopil"),
    "Zakarpatska_Oblast":     ("Uzhhorod, Ukraine",         "Uzhgorod"),
    "Kherson_Oblast":         ("Kherson, Ukraine",          "Kherson"),
    "Khmelnytskyi_Oblast":    ("Khmelnytskyi, Ukraine",     "Khmelnytskyi"),
    "Cherkasy_Oblast":        ("Cherkasy, Ukraine",         "Cherkasy"),
    "Chernivtsi_Oblast":      ("Chernivtsi, Ukraine",       "Chernivtsi"),
    "Chernihiv_Oblast":       ("Chernihiv, Ukraine",        "Chernihiv"),
    "Luhansk_Oblast":         ("Starobilsk, Ukraine",       "Luhansk"),
}

CSV_COLUMNS = [
    "city", "date", "datetime_hour",
    "day_tempmax", "day_tempmin", "day_temp", "day_humidity",
    "day_precip",  "day_windspeed", "day_cloudcover", "day_visibility",
    "day_pressure",
    "hour_temp", "hour_feelslike", "hour_dew", "hour_humidity",
    "hour_windspeed", "hour_winddir", "hour_windgust",
    "hour_visibility", "hour_cloudcover", "hour_pressure",
    "hour_precip", "hour_precipprob", "hour_snow", "hour_snowdepth",
    "hour_conditions",
]

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("weather_collector")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger

def _output_path(target_date: date) -> Path:
    return RAW_NEW_DIR / f"{target_date}_weather_raw.csv"

def _ensure_csv_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

def fetch_region_weather(
    location: str, target_date: date, logger: logging.Logger
) -> dict | None:
    url = BASE_URL.format(
        location=requests.utils.quote(location, safe=""),
        date=target_date.isoformat(),
        key=WEATHER_KEY,
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
                logger.warning(f"Bad request 400 for {location} - skipping")
                return None
            else:
                logger.warning(
                    f"{location} returned {resp.status_code}, attempt {attempt+1}/3"
                )
                time.sleep(10)
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for {location}, attempt {attempt+1}/3")
            time.sleep(10)
        except Exception as e:
            logger.warning(f"Error for {location}: {e}")
            return None
    logger.error(f"All retries failed for {location}")
    return None

def parse_hourly_rows(
    raw: dict, city: str, target_date: date
) -> list[dict]:
    rows = []
    days = raw.get("days", [])
    if not days:
        return rows

    day = days[0]

    day_fields = {
        "day_tempmax":    day.get("tempmax"),
        "day_tempmin":    day.get("tempmin"),
        "day_temp":       day.get("temp"),
        "day_humidity":   day.get("humidity"),
        "day_precip":     day.get("precip"),
        "day_windspeed":  day.get("windspeed"),
        "day_cloudcover": day.get("cloudcover"),
        "day_visibility": day.get("visibility"),
        "day_pressure":   day.get("pressure"),
    }

    for h in day.get("hours", []):
        hour_time = h.get("datetime", "")
        if not hour_time:
            continue
        datetime_hour = f"{target_date} {hour_time[:5]}:00"

        row = {
            "city":          city,
            "date":          str(target_date),
            "datetime_hour": datetime_hour,
            **day_fields,
            "hour_temp":        h.get("temp"),
            "hour_feelslike":   h.get("feelslike"),
            "hour_dew":         h.get("dew"),
            "hour_humidity":    h.get("humidity"),
            "hour_windspeed":   h.get("windspeed"),
            "hour_winddir":     h.get("winddir"),
            "hour_windgust":    h.get("windgust"),
            "hour_visibility":  h.get("visibility"),
            "hour_cloudcover":  h.get("cloudcover"),
            "hour_pressure":    h.get("pressure"),
            "hour_precip":      h.get("precip"),
            "hour_precipprob":  h.get("precipprob"),
            "hour_snow":        h.get("snow"),
            "hour_snowdepth":   h.get("snowdepth"),
            "hour_conditions":  h.get("conditions"),
        }
        rows.append(row)
    return rows

def write_rows(rows: list[dict], path: Path) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerows(rows)

def collect_day(target_date: date, logger: logging.Logger) -> dict:
    out_path = _output_path(target_date)

    if out_path.exists():
        import pandas as pd
        existing = pd.read_csv(out_path)
        if len(existing) >= len(REGIONS) * 24:
            logger.info(f"{target_date} already collected ({len(existing)} rows) - skip")
            return {"regions": len(REGIONS), "hours": len(existing), "errors": 0, "skipped": True}

    _ensure_csv_header(out_path)
    stats = {"regions": 0, "hours": 0, "errors": 0, "skipped": False}
    total = len(REGIONS)

    logger.info(f"Collecting weather for {target_date}  ({total} regions)")
    logger.info(f"Output: {out_path.name}")

    for i, (region, (location, city)) in enumerate(REGIONS.items(), 1):
        logger.info(f"  [{i}/{total}] {region}")
        raw = fetch_region_weather(location, target_date, logger)
        if raw is None:
            stats["errors"] += 1
            logger.error(f"FAILED: {region}")
        else:
            rows = parse_hourly_rows(raw, city, target_date)
            if rows:
                write_rows(rows, out_path)
                stats["hours"]   += len(rows)
                stats["regions"] += 1
                logger.info(f"OK: {len(rows)} hours")
            else:
                logger.warning(f"No hours in response for {region}")
                stats["errors"] += 1

        if i < total:
            time.sleep(SLEEP_BETWEEN)

    return stats

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect daily hourly weather for Ukrainian regions",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Specific date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--days", type=int, default=1,
        help="Collect last N days (default: 1 = yesterday)",
    )
    args = parser.parse_args()

    logger = setup_logging()

    if not WEATHER_KEY:
        logger.error("my_weather_key not set in .env - cannot proceed")
        sys.exit(1)

    if args.date:
        try:
            target_dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
        except ValueError:
            logger.error(f"Invalid date: {args.date} - use YYYY-MM-DD")
            sys.exit(1)
    else:
        today = datetime.now(KYIV_TZ).date()
        target_dates = [today - timedelta(days=d) for d in range(1, args.days + 1)]

    logger.info("-" * 60)
    logger.info("WEATHER COLLECTOR - daily history pull")
    logger.info(f"Dates: {[str(d) for d in target_dates]}")
    logger.info(
        f"Max API cost: {len(REGIONS)} regions × 24h × {len(target_dates)} day(s)"
        f" = {len(REGIONS) * 24 * len(target_dates)} records"
        f"(free tier: 1000/day)"
    )
    logger.info("-" * 60)

    total_hours = 0
    for d in target_dates:
        stats = collect_day(d, logger)
        total_hours += stats["hours"]
        if not stats["skipped"]:
            logger.info(
                f"Date {d}: {stats['hours']} hours across "
                f"{stats['regions']} regions ({stats['errors']} errors)"
            )
        if len(target_dates) > 1 and d != target_dates[-1]:
            logger.info("Waiting 30s before next date...")
            time.sleep(30)

    logger.info(f"DONE - total hours written: {total_hours}")
    logger.info("Next: python data_processing/weather_cleaner_new.py --incremental")

if __name__ == "__main__":
    main()