import json
import argparse
import csv
import hashlib
import logging
import logging.handlers
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
API_KEY = os.getenv("ALERTS_API_KEY")
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "alarms"
OUTPUT_CSV = RAW_DIR / "alarms_new.csv"
LOG_FILE = PROJECT_ROOT / "logs" / "alarms_collector.log"

KYIV_TZ = ZoneInfo("Europe/Kyiv")
UTC_TZ = timezone.utc

HISTORY_URL = "https://api.alerts.in.ua/v1/regions/{uid}/alerts/month_ago.json"
SLEEP_BETWEEN = 31

REGION_UIDS = {
    3:  "Khmelnytskyi Oblast",
    4:  "Vinnytsia Oblast",
    5:  "Rivne Oblast",
    8:  "Volyn Oblast",
    9:  "Dnipropetrovsk Oblast",
    10: "Zhytomyr Oblast",
    11: "Zakarpattia Oblast",
    12: "Zaporizhzhia Oblast",
    13: "Ivano-Frankivsk Oblast",
    14: "Kyiv Oblast",
    15: "Kirovohrad Oblast",
    16: "Luhansk Oblast",
    17: "Mykolaiv Oblast",
    18: "Odesa Oblast",
    19: "Poltava Oblast",
    20: "Sumy Oblast",
    21: "Ternopil Oblast",
    22: "Kharkiv Oblast",
    23: "Kherson Oblast",
    24: "Cherkasy Oblast",
    25: "Chernihiv Oblast",
    26: "Chernivtsi Oblast",
    27: "Lviv Oblast",
    28: "Donetsk Oblast",
    31: "City of Kyiv",
}

REGION_TO_CITY = {
    "Vinnytsia Oblast":        "Вінницька обл.",
    "Volyn Oblast":            "Волинська обл.",
    "Dnipropetrovsk Oblast":   "Дніпропетровська обл.",
    "Donetsk Oblast":          "Донецька обл.",
    "Zhytomyr Oblast":         "Житомирська обл.",
    "Zakarpattia Oblast":      "Закарпатська обл.",
    "Zaporizhzhia Oblast":     "Запорізька обл.",
    "Ivano-Frankivsk Oblast":  "Івано-Франківська обл.",
    "City of Kyiv":            "Київ",
    "Kyiv Oblast":             "Київська обл.",
    "Kirovohrad Oblast":       "Кіровоградська обл.",
    "Luhansk Oblast":          "Луганська обл.",
    "Lviv Oblast":             "Львівська обл.",
    "Mykolaiv Oblast":         "Миколаївська обл.",
    "Odesa Oblast":            "Одеська обл.",
    "Poltava Oblast":          "Полтавська обл.",
    "Rivne Oblast":            "Рівненська обл.",
    "Sumy Oblast":             "Сумська обл.",
    "Ternopil Oblast":         "Тернопільська обл.",
    "Kharkiv Oblast":          "Харківська обл.",
    "Kherson Oblast":          "Херсонська обл.",
    "Khmelnytskyi Oblast":     "Хмельницька обл.",
    "Cherkasy Oblast":         "Черкаська обл.",
    "Chernivtsi Oblast":       "Чернівецька обл.",
    "Chernihiv Oblast":        "Чернігівська обл.",
}

CSV_COLUMNS = [
    "id", "merged_id", "region_id", "region_title",
    "region_city", "all_region", "start", "end", "original_alarms",
]

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents = True, exist_ok = True)
    logger = logging.getLogger("alarms_collector")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount = 3, encoding="utf-8"
        )
        fh.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S",
        ))
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger

def utc_to_kyiv_str(utc_str: str) -> str:
    dt_utc = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
    dt_kyiv = dt_utc.astimezone(KYIV_TZ)
    return dt_kyiv.strftime("%Y-%m-%d %H:%M:%S")

def make_stable_id(region: str, started_at: str) -> int:
    key = f"{region}|{started_at}".encode("utf-8")
    return int(hashlib.md5(key).hexdigest()[:8], 16)

def _csv_is_from_today(logger: logging.Logger) -> bool:
    if not OUTPUT_CSV.exists():
        return False
    mtime = datetime.fromtimestamp(OUTPUT_CSV.stat().st_mtime, tz = KYIV_TZ).date()
    today = datetime.now(KYIV_TZ).date()
    return mtime == today

def prepare_csv(target_dates: list[date], logger: logging.Logger) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    single_day_run = (len(target_dates) == 1)

    if single_day_run and _csv_is_from_today(logger):
        logger.info(
            "alarms_new.csv exists and is from today - appending "
            "(safe resume after crash)"
        )
        return

    with open(OUTPUT_CSV, "w", newline = "", encoding = "utf-8") as f:
        writer = csv.DictWriter(f, fieldnames = CSV_COLUMNS, delimiter = ";")
        writer.writeheader()
    logger.info("Reset alarms_new.csv - fresh file for this run")

def fetch_region_alarms(
    uid: int, region: str, logger: logging.Logger
) -> list[dict] | None:
    if not API_KEY:
        logger.error("ALERTS_API_KEY not set in .env")
        return None

    url = HISTORY_URL.format(uid = uid)
    headers = {"Authorization": f"Bearer {API_KEY}"}

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout = 15)
            if resp.status_code == 200:
                return resp.json().get("alerts", [])
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning(f"Rate limit (429) for uid={uid}, waiting {wait}s")
                time.sleep(wait)
                continue
            elif resp.status_code == 304:
                return []
            else:
                logger.warning(f"uid={uid} returned {resp.status_code}")
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for uid={uid}, attempt {attempt+1}/3")
            time.sleep(10)
        except Exception as e:
            logger.warning(f"Error for uid={uid}: {e}")
            return None

    return None

def alarms_for_date(
        alarms: list[dict], target_date: date, region: str
) -> list[dict]:

    parsed_alarms = []

    target_day_end = datetime(
        target_date.year, target_date.month, target_date.day, 23, 59, 59
    )

    for alarm in alarms:
        if alarm.get("alert_type") != "air_raid":
            continue

        started_utc = alarm.get("started_at")
        if not started_utc:
            continue

        try:
            started_kyiv = utc_to_kyiv_str(started_utc)
            start_dt = datetime.strptime(started_kyiv, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        finished_utc = alarm.get("finished_at")
        if finished_utc:
            finished_kyiv = utc_to_kyiv_str(finished_utc)
            end_dt = datetime.strptime(finished_kyiv, "%Y-%m-%d %H:%M:%S")
        else:
            end_dt = target_day_end

        target_start = datetime.combine(target_date, datetime.min.time())
        target_end = datetime.combine(target_date, datetime.max.time())

        if start_dt <= target_end and end_dt >= target_start:
            parsed_alarms.append({
                "start_dt":      start_dt,
                "end_dt":        end_dt,
                "location_type": alarm.get("location_type", "unknown"),
                "original_id":   str(alarm.get("id")) if alarm.get("id") else "",
                "is_open":       finished_utc is None,
            })

    if not parsed_alarms:
        return []

    parsed_alarms.sort(key=lambda x: x["start_dt"])
    merged_alarms = []

    current = parsed_alarms[0]
    current["ids"] = [current["original_id"]] if current["original_id"] else []
    current["is_whole_oblast"] = (current["location_type"] in ["oblast", "city"])
    current["has_open"] = current["is_open"]

    for nxt in parsed_alarms[1:]:
        if nxt["start_dt"] <= current["end_dt"]:
            current["end_dt"] = max(current["end_dt"], nxt["end_dt"])
            if nxt["original_id"]:
                current["ids"].append(nxt["original_id"])
            if nxt["location_type"] in ["oblast", "city"]:
                current["is_whole_oblast"] = True
            if nxt["is_open"]:
                current["has_open"] = True
        else:
            merged_alarms.append(current)
            current                    = nxt
            current["ids"]             = [current["original_id"]] if current["original_id"] else []
            current["is_whole_oblast"] = (current["location_type"] in ["oblast", "city"])
            current["has_open"]        = current["is_open"]

    merged_alarms.append(current)

    results = []
    for m in merged_alarms:
        results.append({
            "started_kyiv":  m["start_dt"].strftime("%Y-%m-%d %H:%M:%S"),
            "finished_kyiv": m["end_dt"].strftime("%Y-%m-%d %H:%M:%S"),
            "original_ids":  m["ids"],
            "all_region":    1 if m["is_whole_oblast"] else 0,
            "is_open":       m["has_open"],
        })

    return results

def write_alarms(
        region: str, day_alarms: list[dict], logger: logging.Logger
) -> int:
    is_city_of_kyiv = (region == "City of Kyiv")
    region_city = REGION_TO_CITY.get(region, region)
    written = 0

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, delimiter=";")
        for alarm in day_alarms:
            stable_id = make_stable_id(region, alarm["started_kyiv"])
            final_all_region = 0 if is_city_of_kyiv else alarm["all_region"]

            if alarm.get("is_open"):
                logger.info(
                    f"    OPEN alarm {region}: start={alarm['started_kyiv']} "
                    f"end capped at {alarm['finished_kyiv']}"
                )

            row = {
                "id":              stable_id,
                "merged_id":       hashlib.md5(f"{stable_id}".encode()).hexdigest()[:32],
                "region_id":       0,
                "region_title":    "Київська область" if is_city_of_kyiv else region,
                "region_city":     region_city,
                "all_region":      final_all_region,
                "start":           alarm["started_kyiv"],
                "end":             alarm["finished_kyiv"],
                "original_alarms": json.dumps(alarm["original_ids"]) if alarm.get("original_ids") else "[]",
            }
            writer.writerow(row)
            written += 1

    return written

def collect_day(target_date: date, logger: logging.Logger) -> dict:
    stats = {"regions": 0, "alarms": 0, "errors": 0, "open_alarms": 0}
    total_regions = len(REGION_UIDS)

    logger.info(f"Collecting alarms for {target_date}  ({total_regions} regions)")

    for i, (uid, region) in enumerate(REGION_UIDS.items(), 1):
        logger.info(f"  [{i}/{total_regions}] uid={uid} {region}")

        alarms = fetch_region_alarms(uid, region, logger)
        if alarms is None:
            stats["errors"] += 1
        else:
            day_alarms = alarms_for_date(alarms, target_date, region)
            if day_alarms:
                n = write_alarms(region, day_alarms, logger)
                stats["alarms"] += n
                open_n = sum(1 for a in day_alarms if a.get("is_open"))
                stats["open_alarms"] += open_n
                logger.info(
                    f"    wrote {n} alarm(s)"
                    + (f"  ({open_n} open/capped)" if open_n else "")
                )
            else:
                logger.info(f"    no alarms on {target_date}")
            stats["regions"] += 1

        if i < total_regions:
            time.sleep(SLEEP_BETWEEN)

    return stats

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pull yesterday's alarms from alerts.in.ua history API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Specific date to pull YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--days", type=int, default=1,
        help="Pull last N days (default: 1 = yesterday)",
    )
    args = parser.parse_args()

    logger = setup_logging()

    if not API_KEY:
        logger.error("ALERTS_API_KEY not set in .env - cannot proceed")
        sys.exit(1)

    if args.date:
        try:
            target_dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
        except ValueError:
            logger.error(f"Invalid date format: {args.date} - use YYYY-MM-DD")
            sys.exit(1)
    else:
        today = datetime.now(KYIV_TZ).date()
        target_dates = [today - timedelta(days=d) for d in range(1, args.days + 1)]

    logger.info("=" * 60)
    logger.info("ALARMS COLLECTOR — daily history pull")
    logger.info(f"Dates to pull: {[str(d) for d in target_dates]}")
    logger.info(f"Output: {OUTPUT_CSV}")
    logger.info("=" * 60)

    prepare_csv(target_dates, logger)

    total_alarms = 0
    total_open   = 0
    for d in target_dates:
        stats = collect_day(d, logger)
        total_alarms += stats["alarms"]
        total_open   += stats["open_alarms"]
        logger.info(
            f"Date {d}: {stats['alarms']} alarms across "
            f"{stats['regions']} regions ({stats['errors']} errors"
            + (f", {stats['open_alarms']} open/capped)" if stats["open_alarms"] else ")")
        )
        if len(target_dates) > 1 and d != target_dates[-1]:
            logger.info("Waiting 60s before next date...")
            time.sleep(60)

    logger.info(f"DONE - total alarms written: {total_alarms}")
    if total_open:
        logger.info(
            f"NOTE: {total_open} open alarm(s) capped at day boundary."
            "Tomorrow's run will collect correct end time."
            "Processor dedup keep=last updates them automatically."
        )
    logger.info("Next step: python data_processing/alarms_cleaner.py --incremental")

if __name__ == "__main__":
    main()