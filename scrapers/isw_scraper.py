"""
Usage:
    python scrapers/isw_scraper.py --backfill
    python scrapers/isw_scraper.py --daily
    python scrapers/isw_scraper.py --date 2024-06-15
    python scrapers/isw_scraper.py --backfill --force
    python scrapers/isw_scraper.py --backfill --start 2024-01-01 --end 2024-12-31
"""

from __future__ import annotations

import random
import argparse
import json
import logging
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal

from curl_cffi import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

try:
    from zoneinfo import ZoneInfo
    ISW_TIMEZONE = ZoneInfo("America/New_York")
except ImportError:
    ISW_TIMEZONE = timezone(timedelta(hours=-5))

try:
    import lxml
    BS4_PARSER = "lxml"
except ImportError:
    BS4_PARSER = "html.parser"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HTML_DIR = PROJECT_ROOT / "data" / "raw" / "isw_reports" / "html"
TEXT_DIR = PROJECT_ROOT / "data" / "raw" / "isw_reports" / "text"
LOG_DIR  = PROJECT_ROOT / "logs"

_SLUG        = "russian-offensive-campaign-assessment-{month}-{day}-{year}"
_SLUG_PADDED = "russian-offensive-campaign-assessment-{month}-{day_padded}-{year}"
_SLUG_NOYEAR = "russian-offensive-campaign-assessment-{month}-{day}"

PRIMARY_URLS = [
    "https://www.understandingwar.org/backgrounder/" + _SLUG,
    "https://www.understandingwar.org/research/russia-ukraine/" + _SLUG,
]

FALLBACK_URL_PATTERNS = [
    "https://www.understandingwar.org/analysis/russia-ukraine/" + _SLUG,
    "https://www.understandingwar.org/backgrounder/" + _SLUG_PADDED,
    "https://www.understandingwar.org/research/russia-ukraine/" + _SLUG_PADDED,
    "https://www.understandingwar.org/backgrounder/" + _SLUG + "-0",
    "https://www.understandingwar.org/backgrounder/" + _SLUG_NOYEAR,
]

ISW_HOME = "https://www.understandingwar.org/"
BACKFILL_START = datetime(2022, 2, 24)

REQUEST_TIMEOUT = 45
MAX_RETRIES     = 4

MIN_HTML_LENGTH  = 500
MIN_TEXT_LENGTH  = 200
REPORT_VALIDATION_PHRASE = "assessment"

BROWSER_VERSIONS = [
    "chrome110", "chrome116", "chrome119", "chrome120",
    "chrome123", "chrome124",
    "edge101", "edge99",
    "safari15_5", "safari17_0",
]

REFERERS = [
    "https://www.google.com/",
    "https://www.google.com/search?q=isw+russia+ukraine+assessment",
    "https://www.google.com/search?q=institute+study+of+war",
    "https://duckduckgo.com/",
    "https://www.bing.com/search?q=isw+daily+assessment",
    "https://news.google.com/",
    "https://t.co/",
    "https://www.understandingwar.org/",
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.9,uk;q=0.8",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en;q=0.9",
]

MONTH_NAMES = {
    1: "january",   2: "february",  3: "march",
    4: "april",     5: "may",       6: "june",
    7: "july",      8: "august",    9: "september",
    10: "october",  11: "november", 12: "december",
}

RE_SOURCE_REFS  = re.compile(r"\[\s*(?:\d+(?:\s*[,\-]\s*\d+)*|[a-zA-Z]+)\s*\]")
RE_URLS         = re.compile(r"https?://\S+")
RE_MULTI_SPACES = re.compile(r"[ \t]+")

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

def _fmt_kwargs(date: datetime) -> dict[str, str | int]:
    return {
        "month": MONTH_NAMES[date.month],
        "day": date.day,
        "day_padded": f"{date.day:02d}",
        "year": date.year,
    }

def generate_primary_urls(date: datetime) -> list[str]:
    kw = _fmt_kwargs(date)
    return [p.format(**kw) for p in PRIMARY_URLS]

def generate_fallback_urls(date: datetime) -> list[str]:
    kw = _fmt_kwargs(date)
    return [p.format(**kw) for p in FALLBACK_URL_PATTERNS]

def generate_date_range(start: datetime, end: datetime) -> list[datetime]:
    return [start + timedelta(days=x) for x in range((end - start).days + 1)]

def _get_isw_today() -> datetime:
    now = datetime.now(ISW_TIMEZONE).replace(tzinfo=None)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)

def _make_session() -> requests.Session:
    browser = random.choice(BROWSER_VERSIONS)
    s = requests.Session(impersonate=browser)
    s.headers.update({
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Referer": random.choice(REFERERS),
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s

def _warmup_session(session: requests.Session, logger: logging.Logger) -> None:
    try:
        resp = session.get(ISW_HOME, timeout=30, allow_redirects=True)
        if resp.status_code == 200:
            logger.debug("Warmup OK")
        else:
            logger.debug(f"Warmup got {resp.status_code}")
    except Exception:
        logger.debug("Warmup failed (non-critical)")
    time.sleep(random.uniform(0.5, 1.5))


def _human_delay(base_min: float = 5, base_max: float = 12) -> None:
    delay = random.uniform(base_min, base_max)
    if random.random() < 0.12:
        delay += random.uniform(15, 45)
    time.sleep(delay)

def _try_url(url: str, logger: logging.Logger) -> str | None:
    for attempt in range(MAX_RETRIES):
        session = _make_session()

        if attempt == 0 or attempt >= 1:
            _warmup_session(session, logger)

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)

            if resp.status_code == 200:
                html = resp.text
                if _is_cloudflare_page(html):
                    wait = _retry_wait(attempt)
                    logger.warning(
                        f"Cloudflare challenge for {url}. "
                        f"Waiting {wait:.0f}s... (attempt {attempt+1}/{MAX_RETRIES})"
                    )
                    session.close()
                    time.sleep(wait)
                    continue
                session.close()
                return html

            if resp.status_code == 404:
                logger.debug(f"404: {url}")
                session.close()
                return None

            if resp.status_code in (403, 429, 503):
                wait = _retry_wait(attempt)
                logger.warning(
                    f"{resp.status_code} for {url}. "
                    f"Waiting {wait:.0f}s... (attempt {attempt+1}/{MAX_RETRIES})"
                )
                session.close()
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = 15 * (attempt + 1)
                logger.warning(f"{resp.status_code} server error. Waiting {wait}s...")
                session.close()
                time.sleep(wait)
                continue
            logger.debug(f"HTTP {resp.status_code} for {url}. Skipping.")
            session.close()
            return None

        except Exception as e:
            wait = 15 * (attempt + 1)
            logger.warning(f"Error for {url}: {e}. Waiting {wait}s...")
            time.sleep(wait)
        finally:
            try:
                session.close()
            except Exception:
                pass

    logger.debug(f"Failed after {MAX_RETRIES} retries: {url}")
    return None


def _is_cloudflare_page(html: str) -> bool:
    cf_markers = [
        "just a moment",
        "cf-browser-verification",
        "checking your browser",
        "cloudflare",
        "ray id",
        "enable javascript and cookies",
    ]
    lower = html[:3000].lower()
    matches = sum(1 for m in cf_markers if m in lower)
    return matches >= 2


def _retry_wait(attempt: int) -> float:
    base = [15, 30, 45, 60]
    return base[min(attempt, len(base) - 1)] + random.uniform(5, 20)


def download_with_fallback(
    date: datetime, logger: logging.Logger,
) -> tuple[str | None, str]:

    primary_urls = generate_primary_urls(date)
    fallback_urls = generate_fallback_urls(date)

    for url in primary_urls:
        html = _try_url(url, logger)
        if html is not None:
            return html, url
        time.sleep(random.uniform(3, 6))

    for url in fallback_urls:
        logger.debug(f"Fallback: {url}")
        time.sleep(random.uniform(4, 8))
        html = _try_url(url, logger)
        if html is not None:
            logger.info(f"Found at fallback: {url}")
            return html, url

    return None, primary_urls[0]

def _normalize_unicode(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    replacements = {
        "\xa0": " ", "\u200b": "", "\u200c": "", "\u200d": "",
        "\u200e": "", "\u200f": "", "\ufeff": "",
        "\u2013": "-", "\u2014": " - ", "\u2015": " - ", "\u2012": "-",
        "\u2018": "'", "\u2019": "'", "\u201a": "'",
        "\u201c": '"', "\u201d": '"', "\u201e": '"',
        "\u00ab": '"', "\u00bb": '"',
        "\u2026": "...",
        "\u2022": " ", "\u2023": " ", "\u25cf": " ", "\u00b7": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _clean_extracted_text(text: str) -> str:
    text = _normalize_unicode(text)
    text = RE_SOURCE_REFS.sub("", text)
    text = RE_URLS.sub("", text)
    text = RE_MULTI_SPACES.sub(" ", text)

    lines = [line.strip() for line in text.split("\n")]
    cleaned: list[str] = []
    blank = 0
    for line in lines:
        if line == "":
            blank += 1
            if blank <= 2:
                cleaned.append("")
        else:
            blank = 0
            cleaned.append(line)
    return "\n".join(cleaned).strip()


def extract_text_from_html(html_content: str, logger: logging.Logger) -> str | None:
    soup = BeautifulSoup(html_content, BS4_PARSER)

    for tag_name in ("nav", "header", "footer", "script", "style",
                     "noscript", "iframe", "svg", "form"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    noise = (
        "menu", "sidebar", "social", "share", "related",
        "comment", "navigation", "breadcrumb", "search", "banner",
        "popup", "modal", "cookie", "newsletter", "subscribe",
        "advertisement", "ad-container", "footer", "toolbar",
    )
    to_remove = []
    for el in soup.find_all(True):
        classes = " ".join(el.get("class", []))
        combined = f"{classes} {el.get('id', '')}".lower()
        if any(p in combined for p in noise):
            to_remove.append(el)
    for el in to_remove:
        el.decompose()

    content = None
    for selector in [
        {"name": "div", "class_": "field-item"},
        {"name": "div", "class_": "entry-content"},
        {"name": "div", "class_": "node-content"},
        {"name": "article"},
        {"name": "div", "class_": "content"},
        {"name": "main"},
    ]:
        results = soup.find_all(**selector)
        if results:
            content = max(results, key=lambda x: len(x.get_text()))
            break

    if content is None:
        body = soup.find("body")
        if body:
            content = body
        else:
            logger.error("No content in HTML.")
            return None

    raw = content.get_text(separator="\n", strip=True)
    return _clean_extracted_text(raw)

def _safe_write(filepath: Path, content: str, logger: logging.Logger) -> bool:
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(content, encoding="utf-8")
        return True
    except OSError as e:
        logger.error(f"WRITE FAILED: {filepath} - {e}")
        return False

def save_html(html: str, date: datetime, logger: logging.Logger) -> Path | None:
    fp = HTML_DIR / f"{date.strftime('%Y-%m-%d')}.html"
    return fp if _safe_write(fp, html, logger) else None

def save_report_json(text: str, date: datetime, url: str, logger: logging.Logger) -> Path | None:
    fp = TEXT_DIR / f"{date.strftime('%Y-%m-%d')}.json"
    data = {
        "date": date.strftime("%Y-%m-%d"),
        "url": url,
        "status": "ok",
        "char_count": len(text),
        "text": text,
    }
    content = json.dumps(data, ensure_ascii=False, indent=2)
    return fp if _safe_write(fp, content, logger) else None

def file_exists(date: datetime) -> bool:
    return (TEXT_DIR / f"{date.strftime('%Y-%m-%d')}.json").exists()

def scrape_single_report(
    date: datetime, logger: logging.Logger, force: bool = False,
) -> ScrapeResult:
    date_str = date.strftime("%Y-%m-%d")

    if not force and file_exists(date):
        logger.debug(f"SKIP: {date_str}")
        return ScrapeResult(date=date_str, status="skipped", url="")

    html, used_url = download_with_fallback(date, logger)

    if html is None:
        return ScrapeResult(date=date_str, status="not_found", url=used_url)

    if len(html) < MIN_HTML_LENGTH:
        return ScrapeResult(
            date=date_str, status="no_content", url=used_url,
            error_message=f"HTML only {len(html)} bytes",
        )

    save_html(html, date, logger)
    text = extract_text_from_html(html, logger)
    text_len = len(text) if text else 0

    if text is None or text_len < MIN_TEXT_LENGTH:
        return ScrapeResult(
            date=date_str, status="no_content", url=used_url,
            error_message=f"Text {text_len} chars",
        )

    if REPORT_VALIDATION_PHRASE.lower() not in text.lower():
        logger.warning(f"SUSPECT: {date_str} — possibly blocked page")
        return ScrapeResult(
            date=date_str, status="no_content", url=used_url,
            error_message="Missing validation phrase — possibly blocked",
        )

    saved = save_report_json(text, date, used_url, logger)
    if saved is None:
        return ScrapeResult(
            date=date_str, status="error", url=used_url,
            error_message="Write failed",
        )

    logger.info(f"OK: {date_str} — {len(text):,} chars")
    return ScrapeResult(date=date_str, status="ok", url=used_url, chars=len(text))


def _update_stats(stats: ScrapeStats, status: str) -> None:
    attr = {"ok": "ok", "skipped": "skipped", "not_found": "not_found",
            "no_content": "no_content"}.get(status, "error")
    setattr(stats, attr, getattr(stats, attr) + 1)


def scrape_reports(
    dates: list[datetime], logger: logging.Logger, force: bool = False,
) -> ScrapeStats:
    total = len(dates)
    stats = ScrapeStats(total=total)
    failed: list[str] = []

    logger.info(f"Scraping {total} dates: "
                f"{dates[0].strftime('%Y-%m-%d')} -> {dates[-1].strftime('%Y-%m-%d')}")

    is_multi = total > 1
    pbar = tqdm(dates, desc="Scraping ISW", unit="report", disable=not is_multi)

    consecutive_blocks = 0

    for i, date in enumerate(pbar):
        result = scrape_single_report(date, logger, force=force)
        _update_stats(stats, result.status)

        if result.status in ("not_found", "no_content", "error"):
            failed.append(result.date)

        if result.status == "no_content" and "blocked" in result.error_message.lower():
            consecutive_blocks += 1
            if consecutive_blocks >= 3:
                big_wait = random.uniform(90, 180)
                logger.warning(
                    f"{consecutive_blocks} blocks in a row! "
                    f"Long pause {big_wait:.0f}s..."
                )
                time.sleep(big_wait)
                consecutive_blocks = 0
        else:
            consecutive_blocks = 0

        if is_multi:
            pbar.set_postfix(
                ok=stats.ok, skip=stats.skipped,
                fail=stats.not_found + stats.no_content + stats.error,
            )

        if result.status != "skipped" and i < total - 1:
            _human_delay()

    _log_summary(logger, stats, failed, use_tqdm=is_multi)
    return stats


def scrape_daily(logger: logging.Logger, force: bool = False) -> ScrapeStats:
    today = _get_isw_today()
    yesterday = today - timedelta(days=1)
    stats = ScrapeStats(total=1)

    logger.info(f"Mode: DAILY — {today.strftime('%Y-%m-%d')}")
    result = scrape_single_report(today, logger, force=force)

    if result.status in ("ok", "skipped"):
        _update_stats(stats, result.status)
        return stats

    logger.info(f"Today not available. Trying {yesterday.strftime('%Y-%m-%d')}...")
    result = scrape_single_report(yesterday, logger, force=force)
    _update_stats(stats, result.status)
    return stats

def _log_summary(
    logger: logging.Logger, stats: ScrapeStats,
    failed: list[str], use_tqdm: bool = False,
) -> None:
    lines = [
        "", "=" * 60, "SCRAPING COMPLETE",  "=" * 60,
        f"Total:      {stats.total}",
        f"OK:         {stats.ok}",
        f"Skipped:    {stats.skipped}",
        f"Not found:  {stats.not_found}",
        f"No content: {stats.no_content}",
        f"Errors:     {stats.error}",
        "=" * 60,
    ]
    if failed:
        lines.append(f"Failed ({len(failed)}):")
        for d in failed[:20]:
            lines.append(f"  - {d}")
        if len(failed) > 20:
            lines.append(f"  ... +{len(failed) - 20} more")

    for line in lines:
        logger.info(line)
    if use_tqdm:
        for line in lines:
            tqdm.write(line)

def valid_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Bad date: '{s}'. Use YYYY-MM-DD.")

def parse_arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ISW Daily Report Scraper")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--backfill", action="store_true")
    mode.add_argument("--daily", action="store_true")
    mode.add_argument("--date", type=valid_date)
    p.add_argument("--force", action="store_true")
    p.add_argument("--start", type=valid_date)
    p.add_argument("--end", type=valid_date)
    args = p.parse_args()

    if (args.start or args.end) and not args.backfill:
        p.error("--start/--end only with --backfill")
    if args.start and args.end and args.start > args.end:
        p.error("Start must be before end")
    return args

def main() -> None:
    args = parse_arguments()
    logger = setup_logging(verbose=bool(args.daily or args.date))
    isw_now = _get_isw_today()

    logger.info("=" * 60)
    logger.info("ISW SCRAPER START")
    logger.info(f"ISW today: {isw_now.strftime('%Y-%m-%d')} | Parser: {BS4_PARSER}")
    logger.info("=" * 60)

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)

    t0 = time.time()

    if args.daily:
        stats = scrape_daily(logger, force=args.force)

    elif args.backfill:
        start = max(args.start or BACKFILL_START, BACKFILL_START)
        end = min(args.end or isw_now, isw_now)
        if start > isw_now:
            logger.error("Start is in the future.")
            sys.exit(1)
        dates = generate_date_range(start, end)
        logger.info(
            f"BACKFILL: {start.strftime('%Y-%m-%d')} -> "
            f"{end.strftime('%Y-%m-%d')} ({len(dates)} days)"
        )
        stats = scrape_reports(dates, logger, force=args.force)

    elif args.date:
        if args.date < BACKFILL_START:
            logger.error("Date before invasion.")
            sys.exit(1)
        stats = scrape_reports([args.date], logger, force=args.force)

    elapsed = time.time() - t0
    logger.info(
        f"Done in {elapsed:.0f}s ({elapsed/60:.1f}min) | "
        f"OK:{stats.ok} Skip:{stats.skipped} "
        f"NotFound:{stats.not_found} NoContent:{stats.no_content} "
        f"Files: {len(list(HTML_DIR.glob('*.html')))} HTML, "
        f"{len(list(TEXT_DIR.glob('*.json')))} JSON"
    )

    if stats.ok == 0 and stats.skipped == 0:
        if args.daily:
            logger.info("No new report yet. Try later.")
        else:
            logger.error("Nothing downloaded. Check connection.")
            sys.exit(1)

if __name__ == "__main__":
    main()