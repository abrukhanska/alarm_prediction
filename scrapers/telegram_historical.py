"""
Run:
  python scrapers/telegram_historical.py
  python scrapers/telegram_historical.py --since 2023-01-01
  python scrapers/telegram_historical.py --channel monitorwarr
"""
import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from telethon.sync import TelegramClient
    from telethon.tl.types import Message
except ImportError:
    print("Error: 'telethon' library is not installed. Run: pip install telethon")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Error: 'python-dotenv' library is not installed. Run: pip install python-dotenv")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_ROOT / "data" / "raw" / "telegram" / "historical"
LOG_FILE = PROJECT_ROOT / "logs" / "telegram_historical.log"
SESSION_FILE = PROJECT_ROOT / ".telethon_session"

KYIV_TZ = ZoneInfo("Europe/Kyiv")

CHANNELS = [
    "monitorwarr",
    "vanek_nikolaev",
    "kpszsu",
    "GeneralStaff_ua",
    "povitryanatrivogaaa",
    "suspilnenews",
]

BATCH_SIZE = 200

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("tg_historical")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def _progress_path(channel: str) -> Path:
    return OUT_DIR / f".{channel}_progress.json"

def load_progress(channel: str) -> int | None:
    p = _progress_path(channel)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data.get("min_id")
    except (json.JSONDecodeError, KeyError):
        return None

def save_progress(channel: str, min_id: int) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _progress_path(channel).write_text(
        json.dumps({"channel": channel, "min_id": min_id}),
        encoding="utf-8",
    )

def get_output_path(channel: str, since: datetime) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = since.strftime("%Y-%m-%d")
    return OUT_DIR / f"{channel}_since-{date_str}.jsonl"

def fetch_channel(
        client: TelegramClient,
        channel: str,
        since: datetime,
        logger: logging.Logger,
) -> int:
    out_path = get_output_path(channel, since)
    min_id_saved = load_progress(channel)

    fetch_kwargs = {
        "reverse": False,
        "limit": None
    }

    if min_id_saved is not None:
        fetch_kwargs["max_id"] = min_id_saved

    count = 0
    min_id_this_run: int | None = None
    buffer = []

    for msg in client.iter_messages(channel, **fetch_kwargs):
        if not isinstance(msg, Message):
            continue

        if msg.date < since:
            break

        text = (msg.text or "").strip()
        if not text:
            continue

        msg_dt_kyiv = msg.date.astimezone(KYIV_TZ).replace(tzinfo=None)

        record = {
            "msg_id": msg.id,
            "channel": channel,
            "datetime": msg_dt_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
            "text": text,
            "text_len": len(text),
        }

        buffer.append(record)
        count += 1

        if count % 50 == 0:
            time.sleep(random.uniform(0.3, 0.8))

        if min_id_this_run is None or msg.id < min_id_this_run:
            min_id_this_run = msg.id

        if len(buffer) >= BATCH_SIZE:
            with open(out_path, "a", encoding="utf-8") as f:
                for r in buffer:
                    f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
            buffer.clear()
            save_progress(channel, min_id_this_run)
            logger.info(f"Processed {count} messages for {channel}")

    if buffer:
        with open(out_path, "a", encoding="utf-8") as f:
            for r in buffer:
                f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
        buffer.clear()

    if min_id_this_run is not None:
        save_progress(channel, min_id_this_run)

    logger.info(f"Completed {channel}: {count} messages.")
    return count

def main():
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_path)

    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="2022-02-24")
    parser.add_argument("--channel", default=None)
    args = parser.parse_args()

    try:
        since_naive = datetime.strptime(args.since, "%Y-%m-%d")
        since_kyiv = since_naive.replace(hour=0, minute=0, second=0, tzinfo=KYIV_TZ)
        since = since_kyiv.astimezone(timezone.utc)
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    api_id = os.environ.get("TG_API_ID")
    api_hash = os.environ.get("TG_API_HASH")

    if not api_id or not api_hash:
        print("Error: API keys not found in .env!")
        print(f"Searched file here: {env_path.absolute()}")
        print(f"os.environ value for TG_API_ID: {api_id}")
        sys.exit(1)

    try:
        api_id = int(api_id)
    except ValueError:
        print("Error: TG_API_ID must be a number.")
        sys.exit(1)

    print("Keys found, starting client...")

    channels = [args.channel] if args.channel else CHANNELS
    logger = setup_logging()

    with TelegramClient(str(SESSION_FILE), api_id, api_hash, flood_sleep_threshold=300) as client:
        total = 0
        for i, channel in enumerate(channels, 1):
            try:
                count = fetch_channel(client, channel, since, logger)
                total += count
            except Exception as e:
                logger.error(f"Failed to fetch {channel}: {e}")

if __name__ == "__main__":
    main()