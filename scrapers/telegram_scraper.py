"""
Usage:
  python scrapers/telegram_scraper.py --hours 1    # last 1h
  python scrapers/telegram_scraper.py --hours 24   # last 24h
  python scrapers/telegram_scraper.py --hours 720  # backfill ~30d
"""

import argparse
import hashlib
import json
import logging
import logging.handlers
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT  = Path(__file__).resolve().parent.parent
RAW_DIR       = PROJECT_ROOT / "data" / "raw" / "telegram"
LOG_FILE      = PROJECT_ROOT / "logs" / "telegram_scraper.log"
SEEN_IDS_FILE = RAW_DIR / ".seen_ids.json"

KYIV_TZ = ZoneInfo("Europe/Kyiv")

CHANNELS = [
    {
        "username":    "monitorwarr",
        "role":        "strategic_aviation",
        "description": "RF strategic aviation monitoring",
        "priority":    1,
    },
    {
        "username":    "vanek_nikolaev",
        "role":        "tactical_south",
        "description": "Tactical threats from the south",
        "priority":    2,
    },
    {
        "username":    "kpszsu",
        "role":        "official_airforce",
        "description": "Official Ukrainian Air Force Command",
        "priority":    3,
    },
    {
        "username":    "GeneralStaff_ua",
        "role":        "official_gsf",
        "description": "Official General Staff of AFU",
        "priority":    4,
    },
    {
        "username":    "povitryanatrivogaaa",
        "role":        "alarm_aggregator",
        "description": "Air raid alert aggregator for all regions",
        "priority":    5,
    },
    {
        "username":    "suspilnenews",
        "role":        "impact_confirmation",
        "description": "Suspilne News",
        "priority":    6,
    },
]

_DATA_POST_ID_RE = re.compile(r"/(\d+)$")

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("tg_scraper")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
    )
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def _make_dedup_hash(channel: str, text: str, raw_datetime: str) -> str:
    key = f"{channel}|{raw_datetime}|{text[:300]}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]

def load_seen_ids(hours_window: int = 48) -> dict[str, str]:
    if not SEEN_IDS_FILE.exists():
        return {}
    try:
        with open(SEEN_IDS_FILE, encoding="utf-8") as f:
            data: dict[str, str] = json.load(f)
        cutoff = datetime.now(KYIV_TZ).replace(tzinfo=None) - timedelta(hours=hours_window)
        return {
            h: ts for h, ts in data.items()
            if datetime.fromisoformat(ts) >= cutoff
        }
    except (json.JSONDecodeError, KeyError, ValueError):
        return {}

def save_seen_ids(seen: dict[str, str]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    tmp = SEEN_IDS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(seen, f, separators=(",", ":"))
    tmp.replace(SEEN_IDS_FILE)

def _parse_tme_datetime(time_tag) -> datetime | None:
    if time_tag is None:
        return None
    raw = time_tag.get("datetime", "")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(KYIV_TZ).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None

def _fallback_datetime_from_context(msg_div) -> datetime | None:
    date_link = msg_div.select_one("a.tgme_widget_message_date[href]")
    if date_link:
        ts = msg_div.get("data-timestamp")
        if ts and ts.isdigit():
            try:
                dt_utc = datetime.fromtimestamp(int(ts), tz=ZoneInfo("UTC"))
                return dt_utc.astimezone(KYIV_TZ).replace(tzinfo=None)
            except (ValueError, OSError):
                pass
    return None

def _extract_text(msg_div) -> str:
    for sel in ("div.tgme_widget_message_text", "div.tgme_widget_message_caption"):
        el = msg_div.select_one(sel)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if text:
                return text
    return ""

def _fetch_with_retry(
    url: str,
    session: requests.Session,
    channel: str,
    logger: logging.Logger,
    max_retries: int = 3,
) -> requests.Response | None:
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=10)

            if resp.status_code == 200:
                return resp

            if resp.status_code == 429:
                wait = 120 * attempt
                logger.warning(
                    f"  429 Rate Limit @{channel} (attempt {attempt}/{max_retries}) "
                    f"waiting {wait}s"
                )
                time.sleep(wait)
                continue

            if resp.status_code in (403, 404):
                logger.warning(
                    f"  HTTP {resp.status_code} @{channel} channel unavailable, skipping"
                )
                return None

            wait = 5 * attempt
            logger.warning(
                f"  HTTP {resp.status_code} @{channel} "
                f"(attempt {attempt}/{max_retries}) retry in {wait}s"
            )
            time.sleep(wait)

        except requests.Timeout:
            logger.warning(f"  Timeout @{channel} (attempt {attempt}/{max_retries})")
            time.sleep(10)
        except requests.ConnectionError as e:
            logger.warning(f"  ConnectionError @{channel}: {e}")
            time.sleep(15)

    logger.error(f"  @{channel}: all {max_retries} attempts exhausted, skipping")
    return None

def scrape_channel(
    channel: str,
    cutoff_dt: datetime,
    seen_ids: dict[str, str],
    session: requests.Session,
    logger: logging.Logger,
    max_pages: int = 10,
) -> list[dict] | None:
    messages: list[dict] = []
    base_url  = f"https://t.me/s/{channel}"
    before_id: int | None = None

    for page_num in range(max_pages):
        url  = base_url if before_id is None else f"{base_url}?before={before_id}"
        resp = _fetch_with_retry(url, session, channel, logger)

        if resp is None:
            return None if page_num == 0 else messages

        soup     = BeautifulSoup(resp.text, "html.parser")
        msg_divs = soup.select("div.tgme_widget_message")

        if not msg_divs:
            logger.debug(
                f"@{channel} p{page_num + 1}: no messages found "
                f"(channel may be private or empty)"
            )
            break

        found_old   = False
        page_min_id: int | None = None

        for msg_div in reversed(msg_divs):
            data_post  = msg_div.get("data-post", "")
            m          = _DATA_POST_ID_RE.search(data_post)
            msg_id_int = int(m.group(1)) if m else 0

            if msg_id_int > 0:
                page_min_id = (
                    msg_id_int if page_min_id is None
                    else min(page_min_id, msg_id_int)
                )

            time_tag = msg_div.select_one("time[datetime]")
            msg_dt   = _parse_tme_datetime(time_tag) or _fallback_datetime_from_context(msg_div)

            if msg_dt is None:
                logger.debug(f"@{channel} msg_id={msg_id_int}: no datetime, skipping")
                continue

            if msg_dt <= cutoff_dt:
                found_old = True
                break

            text = _extract_text(msg_div)
            if not text:
                logger.warning(
                    f"@{channel} msg_id={msg_id_int}: text is empty. "
                    f"Selectors might be outdated."
                )
                continue

            raw_dt_str  = time_tag.get("datetime", msg_dt.isoformat()) if time_tag else msg_dt.isoformat()
            dedup_hash  = _make_dedup_hash(channel, text, raw_dt_str)

            if dedup_hash in seen_ids:
                continue

            messages.append({
                "id":       f"{channel}_{msg_id_int}",
                "msg_id":   msg_id_int,
                "channel":  channel,
                "datetime": msg_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "text":     text,
                "text_len": len(text),
            })
            seen_ids[dedup_hash] = (
                datetime.now(KYIV_TZ).replace(tzinfo=None).isoformat(timespec="seconds")
            )

        if found_old or page_min_id is None:
            break

        before_id = page_min_id
        time.sleep(1.5)

    logger.info(f"  @{channel}: {len(messages)} new messages")
    return messages

def get_output_path(dt: datetime) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    return RAW_DIR / f"{dt.strftime('%Y-%m-%d')}.jsonl"

def write_messages(messages: list[dict], logger: logging.Logger) -> int:
    if not messages:
        return 0

    out_path = get_output_path(datetime.now(KYIV_TZ).replace(tzinfo=None))
    with open(out_path, "a", encoding="utf-8") as f:
        for msg in messages:
            f.write(json.dumps(msg, ensure_ascii=False, separators=(",", ":")) + "\n")

    logger.info(f"  Wrote {len(messages)} lines to {out_path.name}")
    return len(messages)

def collect(hours_back: int, logger: logging.Logger) -> dict:
    now    = datetime.now(KYIV_TZ).replace(tzinfo=None)
    cutoff = now - timedelta(hours=hours_back)

    dedup_window = max(hours_back * 2, 48)
    seen_ids     = load_seen_ids(hours_window=dedup_window)

    logger.info(f"Collecting last {hours_back}h (cutoff: {cutoff:%Y-%m-%d %H:%M})")
    logger.info(f"Seen IDs in memory: {len(seen_ids)}")
    logger.info(f"Channels to process: {len(CHANNELS)}")

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    })

    stats: dict[str, int] = {
        "channels_ok":   0,
        "channels_fail": 0,
        "messages_new":  0,
    }

    for i, ch_cfg in enumerate(CHANNELS, 1):
        username  = ch_cfg["username"]
        max_pages = max(10, (hours_back * 2) // 5)

        logger.info(f"[{i}/{len(CHANNELS)}] @{username} ({ch_cfg['role']})")

        msgs = scrape_channel(
            channel=username,
            cutoff_dt=cutoff,
            seen_ids=seen_ids,
            session=session,
            logger=logger,
            max_pages=max_pages,
        )

        if msgs is not None:
            write_messages(msgs, logger)
            stats["channels_ok"]  += 1
            stats["messages_new"] += len(msgs)
        else:
            stats["channels_fail"] += 1

        if i < len(CHANNELS):
            time.sleep(2.0)

    save_seen_ids(seen_ids)

    return stats

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hours", type=int, default=1
    )
    args = parser.parse_args()

    if args.hours < 1:
        print("ERROR: --hours must be >= 1", file=sys.stderr)
        sys.exit(1)

    logger = setup_logging()
    logger.info("=" * 60)
    logger.info(f"TELEGRAM SCRAPER | hours_back={args.hours}")
    logger.info("=" * 60)

    stats = collect(args.hours, logger)

    logger.info("")
    logger.info("SUMMARY:")
    logger.info(f"  Channels OK:   {stats['channels_ok']}/{len(CHANNELS)}")
    logger.info(f"  New messages:  {stats['messages_new']}")
    if stats["channels_fail"] > 0:
        logger.warning(f"  Errors:        {stats['channels_fail']} channel(s) skipped")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()