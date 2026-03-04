"""
ISW Daily Report Scraper
========================
Scrapes Russian Offensive Campaign Assessment reports from
https://www.understandingwar.org/ for the period 2022-02-24 to present.

ISW has used TWO different URL structures over time:
  OLD (2022–~2024): /backgrounder/russian-offensive-campaign-assessment-{month}-{day}-{year}
  NEW (2024+):      /research/russia-ukraine/russian-offensive-campaign-assessment-{month}-{day}-{year}/

Confirmed examples:
  https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-march-3-2026/
  https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-march-2-2026/
  https://understandingwar.org/backgrounder/russian-offensive-campaign-assessment-february-24-2022

The scraper tries ALL known formats automatically via fallback logic,
including variants with leading-zero days and update/updated suffixes.

Usage:
    python scrapers/isw_scraper.py --backfill                  # All reports (2022 to today)
    python scrapers/isw_scraper.py --daily                     # Today's / yesterday's report
    python scrapers/isw_scraper.py --date 2024-06-15           # Specific date
    python scrapers/isw_scraper.py --backfill --force          # Re-download existing files
    python scrapers/isw_scraper.py --backfill --start 2024-01-01 --end 2024-12-31

Author: abrukhanska (Team Lead)
Project: Alarm Prediction System — Homework #2
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

try:
    from zoneinfo import ZoneInfo
    ISW_TIMEZONE = ZoneInfo("America/New_York")
except ImportError:
    ISW_TIMEZONE = timezone(timedelta(hours=-5))


PROJECT_ROOT = Path(__file__).resolve().parent.parent

_NEW = "https://www.understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-{month}-{day}-{year}/"
_OLD = "https://www.understandingwar.org/backgrounder/russian-offensive-campaign-assessment-{month}-{day}-{year}"

URL_PATTERNS = [
    _NEW,
    _NEW.rstrip("/") + "-update/",
    _NEW.rstrip("/") + "-updated/",
    _OLD,
    _OLD + "-update",
    _OLD + "-updated",
    "https://www.understandingwar.org/backgrounder/russian-offensive-campaign-assessment-{month}-{day}",
]

BACKFILL_START = datetime(2022, 2, 24)

HTML_DIR = PROJECT_ROOT / "data" / "raw" / "isw_reports" / "html"
TEXT_DIR = PROJECT_ROOT / "data" / "raw" / "isw_reports" / "text"
LOG_DIR = PROJECT_ROOT / "logs"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAYS = [5, 10, 20]
REQUEST_PAUSE = 1.5

MIN_HTML_LENGTH = 500
MIN_TEXT_LENGTH = 1500
REPORT_VALIDATION_KEYWORD = "Russian"

_REF_PATTERN = re.compile(r"\[\d+\]")

_UNICODE_REPLACEMENTS = {
    "\xa0": " ",
    "\u200b": "",
    "\u2009": " ",
    "\u202f": " ",
    "\u200e": "",
    "\u200f": "",
    "\u2013": "-",
    "\u2014": " - ",
    "\u201c": '"',
    "\u201d": '"',
    "\u2018": "'",
    "\u2019": "'",
    "\u2022": "- ",
    "\u2026": "...",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

MONTH_NAMES = {
    1: "january",   2: "february",  3: "march",
    4: "april",     5: "may",       6: "june",
    7: "july",      8: "august",    9: "september",
    10: "october",  11: "november", 12: "december",
}


@dataclass
class ScrapeResult:
    date: str
    status: Literal["ok", "skipped", "not_found", "error", "no_content"]
    url: str
    chars: int = 0
    error_message: str = ""


@dataclass
class ScrapeStats:
    total: int = 0
    ok: int = 0
    skipped: int = 0
    not_found: int = 0
    no_content: int = 0
    error: int = 0


def setup_logging(verbose: bool = False) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("isw_scraper")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if not logger.handlers:
        fh = RotatingFileHandler(
            LOG_DIR / "isw_scraping.log",
            maxBytes=10_000_000, backupCount=3,
            encoding="utf-8", delay=True,
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO if verbose else logging.WARNING)
        ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


def generate_all_urls(date: datetime) -> list[str]:
    month = MONTH_NAMES[date.month]
    day_no_zero = str(date.day)
    day_with_zero = f"{date.day:02d}"
    year = str(date.year)
    urls: list[str] = []
    seen: set[str] = set()
    for pattern in URL_PATTERNS:
        url = pattern.format(month=month, day=day_no_zero, year=year)
        if url not in seen:
            urls.append(url)
            seen.add(url)
        if day_no_zero != day_with_zero:
            url_zero = pattern.format(month=month, day=day_with_zero, year=year)
            if url_zero not in seen:
                urls.append(url_zero)
                seen.add(url_zero)
    return urls


def generate_date_range(start_date: datetime, end_date: datetime) -> list[datetime]:
    n = (end_date - start_date).days + 1
    return [start_date + timedelta(days=x) for x in range(n)]


def _get_isw_today() -> datetime:
    return datetime.now(ISW_TIMEZONE).replace(tzinfo=None)


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _download_one(
    url: str, logger: logging.Logger, session: requests.Session
) -> str | None:
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                logger.debug(f"404: {url}")
                return None
            if r.status_code == 429:
                w = RETRY_DELAYS[attempt] * 3
                logger.warning(f"429 Rate Limited. Waiting {w}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(w); continue
            if r.status_code >= 500:
                w = RETRY_DELAYS[attempt]
                logger.warning(f"{r.status_code} Server Error: {url}. Waiting {w}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(w); continue
            logger.error(f"HTTP {r.status_code}: {url}. Not retrying.")
            return None
        except requests.exceptions.Timeout:
            w = RETRY_DELAYS[attempt]
            logger.warning(f"Timeout: {url}. Waiting {w}s (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(w)
        except requests.exceptions.ConnectionError as e:
            w = RETRY_DELAYS[attempt]
            logger.warning(f"Connection error: {url}: {e}. Waiting {w}s (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(w)
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {url}: {e}")
            return None
    logger.error(f"FAILED after {MAX_RETRIES} retries: {url}")
    return None


def download_report(
    date: datetime, logger: logging.Logger, session: requests.Session
) -> tuple[str | None, str]:
    urls = generate_all_urls(date)
    for i, url in enumerate(urls):
        if i > 0:
            logger.debug(f"Trying fallback URL: {url}")
        html = _download_one(url, logger, session)
        if html is not None:
            if i > 0:
                logger.info(f"Found at fallback URL: {url}")
            return html, url
    return None, urls[0]


def extract_text_from_html(html_content: str, logger: logging.Logger) -> str | None:
    soup = BeautifulSoup(html_content, "lxml")
    for tag_name in ("nav", "header", "footer", "script", "style", "noscript", "iframe"):
        for tag in soup.find_all(tag_name):
            tag.decompose()
    noise_patterns = (
        "menu", "sidebar", "social", "share", "related",
        "comment", "navigation", "breadcrumb", "search", "banner",
        "popup", "modal", "cookie", "newsletter", "subscribe",
        "advertisement", "ad-container", "ad-wrapper", "ad-slot",
    )
    to_remove = []
    for el in soup.find_all(True):
        classes = " ".join(el.get("class", []))
        eid = el.get("id", "")
        combined = f"{classes} {eid}".lower()
        if any(p in combined for p in noise_patterns):
            to_remove.append(el)
    for el in to_remove:
        el.decompose()
    content = None
    for selector in (
        {"name": "div", "class_": "field-item"},
        {"name": "div", "class_": "entry-content"},
        {"name": "div", "class_": "node-content"},
        {"name": "article"},
        {"name": "div", "class_": "content"},
        {"name": "main"},
    ):
        results = soup.find_all(**selector)
        if results:
            content = max(results, key=lambda x: len(x.get_text()))
            break
    if content is None:
        body = soup.find("body")
        if body:
            content = body
            logger.warning("No specific content block found. Using <body>.")
        else:
            logger.error("No content found in HTML at all.")
            return None
    text = content.get_text(separator="\n", strip=True)
    text = _REF_PATTERN.sub("", text)
    for char, replacement in _UNICODE_REPLACEMENTS.items():
        text = text.replace(char, replacement)
    lines = text.split("\n")
    cleaned: list[str] = []
    blanks = 0
    for line in lines:
        s = line.strip()
        if s == "":
            blanks += 1
            if blanks <= 2:
                cleaned.append("")
        else:
            blanks = 0
            cleaned.append(s)
    return "\n".join(cleaned).strip()


def save_html(html_content: str, date: datetime) -> Path:
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    fp = HTML_DIR / f"{date.strftime('%Y-%m-%d')}.html"
    fp.write_text(html_content, encoding="utf-8")
    return fp


def save_report_json(text: str, date: datetime, url: str) -> Path:
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    fp = TEXT_DIR / f"{date.strftime('%Y-%m-%d')}.json"
    fp.write_text(
        json.dumps(
            {
                "date": date.strftime("%Y-%m-%d"),
                "url": url,
                "status": "ok",
                "char_count": len(text),
                "text": text,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return fp


def file_exists(date: datetime) -> bool:
    return (TEXT_DIR / f"{date.strftime('%Y-%m-%d')}.json").exists()


def scrape_single_report(
    date: datetime,
    logger: logging.Logger,
    session: requests.Session,
    force: bool = False,
) -> ScrapeResult:
    ds = date.strftime("%Y-%m-%d")
    if not force and file_exists(date):
        logger.debug(f"SKIP: {ds} — already exists")
        return ScrapeResult(date=ds, status="skipped", url="")
    html, url = download_report(date, logger, session)
    if html is None:
        logger.info(f"NOT FOUND: {ds}")
        return ScrapeResult(date=ds, status="not_found", url=url)
    if len(html) < MIN_HTML_LENGTH:
        logger.warning(f"GARBAGE HTML: {ds} — {len(html)} bytes. Not saving.")
        return ScrapeResult(date=ds, status="no_content", url=url,
                            error_message=f"HTML only {len(html)} bytes")
    save_html(html, date)
    text = extract_text_from_html(html, logger)
    tlen = len(text) if text else 0
    if text is None or tlen < MIN_TEXT_LENGTH:
        logger.warning(f"NO CONTENT: {ds} — {tlen} chars (need {MIN_TEXT_LENGTH}+)")
        return ScrapeResult(date=ds, status="no_content", url=url,
                            error_message=f"Text {tlen} chars (min {MIN_TEXT_LENGTH})")
    if REPORT_VALIDATION_KEYWORD.lower() not in text.lower():
        logger.warning(f"INVALID CONTENT: {ds} — missing '{REPORT_VALIDATION_KEYWORD}'")
        return ScrapeResult(date=ds, status="no_content", url=url,
                            error_message=f"Missing keyword '{REPORT_VALIDATION_KEYWORD}'")
    save_report_json(text, date, url)
    cc = len(text)
    logger.info(f"OK: {ds} — {cc:,} chars")
    return ScrapeResult(date=ds, status="ok", url=url, chars=cc)


def scrape_daily(
    logger: logging.Logger,
    session: requests.Session,
    force: bool = False,
) -> ScrapeStats:
    today = _get_isw_today()
    yesterday = today - timedelta(days=1)
    stats = ScrapeStats(total=1)
    logger.info(f"Mode: DAILY — trying {today.strftime('%Y-%m-%d')} (ISW timezone)")
    r = scrape_single_report(today, logger, session, force=force)
    if r.status == "ok":
        stats.ok = 1
        return stats
    if r.status == "skipped":
        stats.skipped = 1
        return stats
    logger.info(
        f"Today's report not available ({r.status}). "
        f"Falling back to {yesterday.strftime('%Y-%m-%d')}..."
    )
    r = scrape_single_report(yesterday, logger, session, force=force)
    if r.status == "ok":
        stats.ok = 1
    elif r.status == "skipped":
        stats.skipped = 1
        logger.info(
            "No new report found. Yesterday's is already cached. "
            "Try running later when ISW publishes today's assessment."
        )
    elif r.status == "not_found":
        stats.not_found = 1
    elif r.status == "no_content":
        stats.no_content = 1
    else:
        stats.error = 1
    return stats


def _update_stats(stats: ScrapeStats, status: str) -> None:
    counter_map = {
        "ok": "ok", "skipped": "skipped", "not_found": "not_found",
        "no_content": "no_content", "error": "error",
    }
    attr = counter_map.get(status, "error")
    setattr(stats, attr, getattr(stats, attr) + 1)


def scrape_reports(
    dates: list[datetime],
    logger: logging.Logger,
    session: requests.Session,
    force: bool = False,
) -> ScrapeStats:
    total = len(dates)
    stats = ScrapeStats(total=total)
    failed: list[str] = []
    logger.info(f"Starting scraping of {total} dates...")
    logger.info(f"Range: {dates[0].strftime('%Y-%m-%d')} → {dates[-1].strftime('%Y-%m-%d')}")
    it = tqdm(dates, desc="Scraping ISW", unit="report", disable=(total <= 1))
    for i, date in enumerate(it):
        r = scrape_single_report(date, logger, session, force=force)
        _update_stats(stats, r.status)
        if r.status in ("not_found", "no_content", "error"):
            failed.append(r.date)
        if total > 1:
            it.set_postfix(ok=stats.ok, skip=stats.skipped,
                           fail=stats.not_found + stats.no_content + stats.error)
        if r.status != "skipped" and i < total - 1:
            time.sleep(REQUEST_PAUSE)
    _log_summary(logger, stats, failed)
    return stats


def _log_summary(
    logger: logging.Logger, stats: ScrapeStats, failed: list[str]
) -> None:
    logger.info("=" * 60)
    logger.info("SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total:      {stats.total}")
    logger.info(f"Saved:      {stats.ok}")
    logger.info(f"Skipped:    {stats.skipped}")
    logger.info(f"Not found:  {stats.not_found}")
    logger.info(f"No content: {stats.no_content}")
    logger.info(f"Errors:     {stats.error}")
    logger.info("=" * 60)
    if failed:
        logger.info(f"Failed dates ({len(failed)}):")
        for d in failed[:20]:
            logger.info(f"  - {d}")
        if len(failed) > 20:
            logger.info(f"  ... and {len(failed) - 20} more")


def _valid_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date: '{s}'. Use YYYY-MM-DD.")


def parse_arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ISW Daily Report Scraper — Alarm Prediction System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrapers/isw_scraper.py --backfill
  python scrapers/isw_scraper.py --daily
  python scrapers/isw_scraper.py --date 2024-06-15
  python scrapers/isw_scraper.py --backfill --start 2024-01-01 --end 2024-12-31
        """,
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--backfill", action="store_true",
                       help="All reports from 2022-02-24 to today")
    mode.add_argument("--daily", action="store_true",
                       help="Today's report (smart fallback to yesterday)")
    mode.add_argument("--date", type=_valid_date,
                       help="Specific date (YYYY-MM-DD)")
    p.add_argument("--force", action="store_true",
                    help="Re-download existing files")
    p.add_argument("--start", type=_valid_date,
                    help="Custom backfill start (YYYY-MM-DD)")
    p.add_argument("--end", type=_valid_date,
                    help="Custom backfill end (YYYY-MM-DD)")
    args = p.parse_args()
    if (args.start or args.end) and not args.backfill:
        p.error("--start/--end require --backfill")
    if args.start and args.end and args.start > args.end:
        p.error(f"Start ({args.start:%Y-%m-%d}) must be before end ({args.end:%Y-%m-%d})")
    return args


def main() -> None:
    args = parse_arguments()
    is_single = bool(args.daily or args.date)
    logger = setup_logging(verbose=is_single)
    logger.info("=" * 60)
    logger.info("ISW SCRAPER — STARTING")
    logger.info("=" * 60)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with create_session() as session:
        if args.daily:
            if args.force:
                logger.info("Force mode ON")
            stats = scrape_daily(logger, session, force=args.force)
        elif args.backfill:
            isw_now = _get_isw_today()
            start = args.start if args.start else BACKFILL_START
            end = args.end if args.end else isw_now
            if start < BACKFILL_START:
                logger.warning(
                    f"Start date {start:%Y-%m-%d} is before the invasion. "
                    f"Clamping to {BACKFILL_START:%Y-%m-%d}."
                )
                start = BACKFILL_START
            if start > isw_now:
                logger.error(f"Start {start:%Y-%m-%d} is in the future (ISW today: {isw_now:%Y-%m-%d}).")
                sys.exit(1)
            dates = generate_date_range(start, end)
            logger.info(f"Mode: BACKFILL ({start:%Y-%m-%d} → {end:%Y-%m-%d})")
            logger.info(f"Total dates: {len(dates)}")
            if args.force:
                logger.info("Force mode ON")
            stats = scrape_reports(dates, logger, session, force=args.force)
        elif args.date:
            logger.info(f"Mode: SINGLE DATE ({args.date:%Y-%m-%d})")
            if args.force:
                logger.info("Force mode ON")
            stats = scrape_reports([args.date], logger, session, force=args.force)
    elapsed = time.time() - t0
    html_n = len(list(HTML_DIR.glob("*.html")))
    json_n = len(list(TEXT_DIR.glob("*.json")))
    summary = (
        f"\n{'=' * 50}\n"
        f"  Done in {elapsed:.1f}s ({elapsed / 60:.1f} min)\n"
        f"  OK: {stats.ok} | Skipped: {stats.skipped} | "
        f"Not found: {stats.not_found} | No content: {stats.no_content}\n"
        f"  Files on disk: {html_n} HTML, {json_n} JSON\n"
        f"{'=' * 50}"
    )
    logger.info(summary)
    if not is_single:
        tqdm.write(summary)
    if stats.ok == 0 and stats.skipped == 0:
        if args.daily:
            logger.info("No new report yet. This is normal — try later.")
            sys.exit(0)
        else:
            logger.error("No reports downloaded. Check internet connection.")
            sys.exit(1)
    logger.info("Done! ✅")


if __name__ == "__main__":
    main()
