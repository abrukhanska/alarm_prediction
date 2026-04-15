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
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from telethon.sync import TelegramClient
    from telethon.tl.types import Message
except ImportError:
    print(
        "ERROR: Telethon is not installed.\n"
        "Run:  pip install telethon python-dotenv\n"
        "Then: python scrapers/telegram_historical.py",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print(
        "ERROR: python-dotenv is not installed.\n"
        "Run:  pip install telethon python-dotenv\n"
        "Then: python scrapers/telegram_historical.py",
        file=sys.stderr,
    )
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR      = PROJECT_ROOT / "data" / "raw" / "telegram" / "historical"
LOG_FILE     = PROJECT_ROOT / "logs" / "telegram_historical.log"
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

BATCH_SIZE = 3000

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
    out_path   = get_output_path(channel, since)
    min_id_saved = load_progress(channel)

    fetch_kwargs = {
        "reverse": False,
        "limit": None,
        "wait_time": 1
    }

    if min_id_saved is not None:
        logger.info(f"  @{channel}: resuming from msg_id < {min_id_saved}")
        fetch_kwargs["max_id"] = min_id_saved
    else:
        logger.info(f"  @{channel}: full fetch from {since.date()}")

    count = 0
    min_id_this_run: int | None = None

    with open(out_path, "a", encoding="utf-8") as f:

        for msg in client.iter_messages(channel, **fetch_kwargs):
            if not isinstance(msg, Message):
                continue

            msg_dt_kyiv = msg.date.astimezone(KYIV_TZ).replace(tzinfo=None)

            if msg.date < since:
                break

            text = (msg.message or "").strip()
            if not text:
                continue

            record = {
                "msg_id":   msg.id,
                "channel":  channel,
                "datetime": msg_dt_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
                "text":     text,
                "text_len": len(text),
            }
            f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1

            if min_id_this_run is None or msg.id < min_id_this_run:
                min_id_this_run = msg.id

            if count % 1000 == 0:
                logger.info(f"  @{channel}: {count:,} messages written (current date: {msg_dt_kyiv.strftime('%Y-%m-%d')})")
                save_progress(channel, min_id_this_run)

    if min_id_this_run is not None:
        save_progress(channel, min_id_this_run)

    logger.info(f"  @{channel}: DONE — {count:,} messages → {out_path.name}")
    return count

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="AEGIS Historical Backfill — Telethon MTProto (run on laptop)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrapers/telegram_historical.py
  python scrapers/telegram_historical.py --since 2023-06-01
  python scrapers/telegram_historical.py --channel air_alert_ua

Environment variables required:
  TG_API_ID    — from https://my.telegram.org (numeric)
  TG_API_HASH  — from https://my.telegram.org (hex string)
        """,
    )
    parser.add_argument(
        "--since",
        default="2022-02-24",
        help="Start date in YYYY-MM-DD format (default: 2022-02-24)",
    )
    parser.add_argument(
        "--channel",
        default=None,
        help="Fetch only this channel (default: all 6 channels)",
    )
    args = parser.parse_args()

    try:
        since_naive = datetime.strptime(args.since, "%Y-%m-%d")
        since_kyiv = since_naive.replace(hour=4, tzinfo=KYIV_TZ)
        since = since_kyiv.astimezone(timezone.utc)
    except ValueError:
        print(f"ERROR: --since must be YYYY-MM-DD, got: {args.since}", file=sys.stderr)
        sys.exit(1)

    api_id   = os.environ.get("TG_API_ID")
    api_hash = os.environ.get("TG_API_HASH")

    if not api_id or not api_hash:
        print(
            "ERROR: Set TG_API_ID and TG_API_HASH environment variables.\n"
            "Get them at: https://my.telegram.org -> API development tools",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        api_id = int(api_id)
    except ValueError:
        print("ERROR: TG_API_ID must be a number.", file=sys.stderr)
        sys.exit(1)

    channels = [args.channel] if args.channel else CHANNELS

    logger = setup_logging()
    logger.info("=" * 60)
    logger.info(f"HISTORICAL BACKFILL | since={args.since} | channels={channels}")
    logger.info("=" * 60)
    logger.info("Starting Telegram client — you may be asked to enter your")
    logger.info("phone number and SMS code on the FIRST run only.")
    logger.info(f"Session saved to: {SESSION_FILE}")

    with TelegramClient(str(SESSION_FILE), api_id, api_hash, flood_sleep_threshold=60) as client:
        total = 0
        for i, channel in enumerate(channels, 1):
            logger.info(f"[{i}/{len(channels)}] Fetching @{channel} since {args.since}")
            try:
                count = fetch_channel(client, channel, since, logger)
                total += count
            except Exception as exc:
                logger.error(f"  @{channel}: unexpected error — {exc}")
                logger.error("  Skipping this channel, continuing with the next.")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"BACKFILL COMPLETE — {total:,} total messages saved")
    logger.info(f"Output directory: {OUT_DIR}")
    logger.info("Next step: python data_processing/telegram_nlp.py --build")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()