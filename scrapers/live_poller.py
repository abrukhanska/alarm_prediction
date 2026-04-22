import json
import logging
import logging.handlers
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ALERTS_API_KEY")
LIVE_DIR = PROJECT_ROOT / "data" / "live"
OUTPUT_JSON = LIVE_DIR / "live_state.json"
LOG_FILE = PROJECT_ROOT / "logs" / "live_poller.log"

API_URL = "https://api.alerts.in.ua/v1/alerts/active.json"
KYIV_TZ = ZoneInfo("Europe/Kyiv")

REGION_MAPPING = {
    "Вінницька область": "Vinnytsia Oblast",
    "Волинська область": "Volyn Oblast",
    "Дніпропетровська область": "Dnipropetrovsk Oblast",
    "Донецька область": "Donetsk Oblast",
    "Житомирська область": "Zhytomyr Oblast",
    "Закарпатська область": "Zakarpattia Oblast",
    "Запорізька область": "Zaporizhzhia Oblast",
    "Івано-Франківська область": "Ivano-Frankivsk Oblast",
    "м. Київ": "City of Kyiv",
    "Київська область": "Kyiv Oblast",
    "Кіровоградська область": "Kirovohrad Oblast",
    "Луганська область": "Luhansk Oblast",
    "Львівська область": "Lviv Oblast",
    "Миколаївська область": "Mykolaiv Oblast",
    "Одеська область": "Odesa Oblast",
    "Полтавська область": "Poltava Oblast",
    "Рівненська область": "Rivne Oblast",
    "Сумська область": "Sumy Oblast",
    "Тернопільська область": "Ternopil Oblast",
    "Харківська область": "Kharkiv Oblast",
    "Херсонська область": "Kherson Oblast",
    "Хмельницька область": "Khmelnytskyi Oblast",
    "Черкаська область": "Cherkasy Oblast",
    "Чернівецька область": "Chernivtsi Oblast",
    "Чернігівська область": "Chernihiv Oblast"
}

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("live_poller")
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

def main():
    logger = setup_logging()

    if not API_KEY:
        logger.error("ALERTS_API_KEY missing in .env!")
        sys.exit(1)

    LIVE_DIR.mkdir(parents=True, exist_ok=True)

    state = {eng_name: False for eng_name in REGION_MAPPING.values()}

    try:
        resp = requests.get(
            API_URL,
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=10
        )
        resp.raise_for_status()
        alerts = resp.json().get("alerts", [])

    except requests.exceptions.RequestException as e:
        logger.error(f"API Fetch failed: {e}. Aborting execution, keeping last cache.")
        sys.exit(1)

    for alert in alerts:
        if alert.get("alert_type") == "air_raid":
            ua_name = alert.get("location_oblast") or alert.get("location_title")
            eng_name = REGION_MAPPING.get(ua_name)

            if eng_name:
                state[eng_name] = True

    active_count = sum(1 for is_active in state.values() if is_active)

    output_data = {
        "poll_time_kyiv": datetime.now(KYIV_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "active_alarms_count": active_count,
        "regions": state
    }

    temp_path = None
    try:
        fd, temp_path = tempfile.mkstemp(dir=LIVE_DIR, text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        os.replace(temp_path, OUTPUT_JSON)
        temp_path = None
    except Exception as e:
        logger.error(f"Error writing JSON: {e}")
        sys.exit(1)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
            logger.info("Temporary file successfully deleted after error.")

    logger.info(f"Live state updated. Active regions: {active_count}")

if __name__ == "__main__":
    main()