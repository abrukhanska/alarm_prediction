"""
Extracts external source URLs from locally saved ISW report HTML files
and resolves their titles via HTTP requests (with global URL cache).
"""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

try:
    import lxml
    BS4_PARSER = "lxml"
except ImportError:
    BS4_PARSER = "html.parser"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HTML_DIR = PROJECT_ROOT / "data" / "raw" / "isw_reports" / "html"
SOURCES_DIR = PROJECT_ROOT / "data" / "raw" / "isw_sources"
LOG_DIR = PROJECT_ROOT / "logs"
CACHE_PATH = PROJECT_ROOT / "data" / "processed" / "sources_cache.json"

REQUEST_TIMEOUT = 15
MAX_RETRIES = 2
RETRY_DELAY = 3
REQUEST_PAUSE = 0.5
MAX_REDIRECTS = 5
MAX_RESPONSE_SIZE = 5_000_000
CACHE_SAVE_EVERY = 10

ISW_DOMAINS = {
    "understandingwar.org",
    "www.understandingwar.org",
    "isw.pub",
    "www.isw.pub",
    "iswresearch.org",
    "www.iswresearch.org",
}
SKIP_DOMAINS = {
    "tiktok.com", "www.tiktok.com",
    "linkedin.com", "www.linkedin.com",
}
NON_HTML_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".bmp",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".zip", ".rar", ".7z", ".tar", ".gz",
    ".csv", ".xml", ".rss",
}
NON_HTML_CONTENT_TYPES = {
    "application/pdf",
    "application/octet-stream",
    "image/",
    "video/",
    "audio/",
    "application/zip",
    "application/x-rar",
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

RE_MULTI_SPACES = re.compile(r"\s+")
TELEGRAM_BLACKLIST = {"joinchat", "addstickers", "share", "invoice"}

RE_FOOTNOTE_MARKER = re.compile(
    r"\[\s*\d+\s*\]"
)

@dataclass
class SourceEntry:
    url: str
    title: str
    status: Literal[
        "ok", "dead_link", "timeout", "connection_error",
        "non_html", "blocked", "redirect_loop", "error",
        "skipped", "unresolved",
    ]
    http_code: int = 0
    content_type: str = ""
    final_url: str = ""
    error_detail: str = ""

    def to_dict(self) -> dict:
        result = {
            "url": self.url,
            "title": self.title,
            "status": self.status,
        }
        if self.http_code:
            result["http_code"] = self.http_code
        if self.content_type:
            result["content_type"] = self.content_type
        if self.final_url and self.final_url != self.url:
            result["final_url"] = self.final_url
        if self.error_detail:
            result["error_detail"] = self.error_detail
        return result


@dataclass
class ReportSources:
    report_date: str
    html_file: str
    sources_count: int = 0
    resolved_count: int = 0
    dead_count: int = 0
    blocked_count: int = 0
    non_html_count: int = 0
    sources: list[SourceEntry] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "report_date": self.report_date,
            "html_file": self.html_file,
            "sources_count": self.sources_count,
            "resolved_count": self.resolved_count,
            "dead_count": self.dead_count,
            "blocked_count": self.blocked_count,
            "non_html_count": self.non_html_count,
            "sources": [s.to_dict() for s in self.sources],
        }


@dataclass
class ProcessStats:
    total_reports: int = 0
    processed: int = 0
    skipped: int = 0
    total_sources: int = 0
    resolved: int = 0
    dead: int = 0
    blocked: int = 0
    non_html: int = 0
    errors: int = 0

def setup_logging(verbose: bool = False) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "isw_sources.log"

    logger = logging.getLogger("isw_sources")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if not logger.handlers:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10_000_000,
            backupCount=3, encoding="utf-8", delay=True,
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG if verbose else logging.WARNING)
        console_handler.setFormatter(
            logging.Formatter("%(levelname)-7s | %(message)s")
        )

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

def _get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def _is_isw_internal(url: str) -> bool:
    domain = _get_domain(url)
    return any(domain == d or domain.endswith("." + d) for d in ISW_DOMAINS)

def _is_blocked_domain(url: str) -> bool:
    domain = _get_domain(url)
    return any(domain == d or domain.endswith("." + d) for d in SKIP_DOMAINS)

def _is_non_html_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in NON_HTML_EXTENSIONS)
    except Exception:
        return False

def _is_valid_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False

def _clean_url(url: str) -> str:
    url = url.strip()
    if "#" in url:
        url = url.split("#")[0]
    url = re.sub(r'[.,;:)}\]]+$', '', url)
    url = url.rstrip('/')
    return url

def _find_footnote_section(soup: BeautifulSoup) -> Tag | None:
    printable = soup.find("div", id="printable-area")
    if printable:
        return printable

    field_items = soup.find_all("div", class_="field-item")
    if field_items:
        if len(field_items) == 1:
            return field_items[0]
        wrapper = soup.new_tag("div", attrs={"class": "merged-content"})
        for fi in field_items:
            for child in fi.children:
                wrapper.append(child.__copy__() if hasattr(child, '__copy__') else child)
        return wrapper

    content_classes = [
        "dynamic-entry-content", "entry-content", "node-content",
        "article-content", "post-content"
    ]
    for class_name in content_classes:
        results = soup.find_all("div", class_=class_name)
        if results:
            if len(results) == 1:
                return results[0]
            wrapper = soup.new_tag("div")
            for r in results:
                for child in r.children:
                    wrapper.append(child.__copy__() if hasattr(child, '__copy__') else child)
            return wrapper

    for header_text in ["endnote", "footnote", "reference", "source"]:
        for tag in soup.find_all(["h2", "h3", "h4", "p", "strong"]):
            if tag.get_text(strip=True).lower().startswith(header_text):
                parent = tag.find_parent(["div", "section", "article"])
                if parent:
                    return parent

    for selector in ["article", "main", '[role="main"]']:
        tag = soup.find(selector)
        if tag:
            return tag

    return None


def extract_urls_from_html(
        html_path: Path, logger: logging.Logger
) -> list[str]:
    try:
        with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
            html_content = f.read()
    except Exception as e:
        logger.error(f"Cannot read {html_path.name}: {e}")
        return []

    soup = BeautifulSoup(html_content, BS4_PARSER)
    content = _find_footnote_section(soup)

    if content is None:
        content = soup

    raw_urls: list[str] = []

    for a_tag in content.find_all("a", href=True):
        raw_urls.append(a_tag["href"])

    full_text = content.get_text()
    text_urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', full_text)
    raw_urls.extend(text_urls)

    seen: set[str] = set()
    unique_urls: list[str] = []

    for url in raw_urls:
        url = _clean_url(url)

        if not _is_valid_url(url):
            continue

        if _is_isw_internal(url):
            continue

        if url in seen:
            continue

        seen.add(url)
        unique_urls.append(url)

    logger.debug(f"{html_path.name}: Found {len(unique_urls)} unique external sources")
    return unique_urls

def _extract_title_from_html(html: str) -> str:
    soup = BeautifulSoup(html, BS4_PARSER)
    title_tag = soup.find("title")
    if title_tag is None:
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return RE_MULTI_SPACES.sub(" ", og_title["content"]).strip()[:200]
        return "[NO TITLE]"

    title = title_tag.get_text(strip=True)
    if not title:
        return "[EMPTY TITLE]"

    title = RE_MULTI_SPACES.sub(" ", title).strip()
    if len(title) > 200:
        title = title[:197] + "..."
    return title

def _is_non_html_content_type(content_type: str) -> bool:
    ct = content_type.lower()
    return any(ct.startswith(prefix) for prefix in NON_HTML_CONTENT_TYPES)

def _special_title(url: str) -> tuple[str, str]:
    domain = _get_domain(url)

    if "t.me" in domain or "telegram" in domain:
        match = re.search(r't\.me/(?:s/)?([a-zA-Z0-9_]+)(?:/(\d+))?', url)
        if match:
            nick = match.group(1)
            postid = match.group(2)
            if nick.lower() in TELEGRAM_BLACKLIST:
                return "[Telegram]", "blocked"
            post = f" (Post {postid})" if postid else ""
            return f"[Telegram: @{nick}{post}]", "ok"
        return "[Telegram]", "blocked"

    if domain in ("x.com", "twitter.com", "www.x.com", "www.twitter.com"):
        match = re.search(
            r'(?:x|twitter)\.com/(?:#!/)?(\w+)/status(?:es)?/(\d+)', url
        )
        if match:
            user, tweetid = match.group(1), match.group(2)
            if user.lower() in ('i', 'search', 'explore', 'home'):
                return "[X]", "blocked"
            return f"[X: @{user} (Tweet {tweetid})]", "ok"
        match_profile = re.search(r'(?:x|twitter)\.com/(\w+)', url)
        if match_profile:
            username = match_profile.group(1)
            if username.lower() not in ('i', 'search', 'explore', 'home', 'intent'):
                return f"[X: @{username}]", "ok"
        return "[X]", "blocked"

    if _is_blocked_domain(url):
        return f"[BLOCKED: {domain}]", "blocked"

    if _is_non_html_url(url):
        ext = Path(urlparse(url).path).suffix.upper()
        return f"[NON-HTML: {ext}]", "non_html"

    return "", ""

def resolve_source_title(
    url: str, session: requests.Session, logger: logging.Logger
) -> SourceEntry:
    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                stream=True,
            )

            content_type = response.headers.get("Content-Type", "")

            if _is_non_html_content_type(content_type):
                response.close()
                return SourceEntry(
                    url=url,
                    title=f"[NON-HTML: {content_type.split(';')[0].strip()}]",
                    status="non_html",
                    http_code=response.status_code,
                    content_type=content_type.split(";")[0].strip(),
                    final_url=response.url,
                )

            content_length = response.headers.get("Content-Length", "")
            if content_length and int(content_length) > MAX_RESPONSE_SIZE:
                response.close()
                return SourceEntry(
                    url=url,
                    title="[TOO LARGE]",
                    status="non_html",
                    http_code=response.status_code,
                    error_detail=f"Content-Length: {content_length}",
                )

            body = b""
            for chunk in response.iter_content(chunk_size=8192):
                body += chunk
                if len(body) > MAX_RESPONSE_SIZE:
                    break
            response.close()

            if response.status_code == 404:
                return SourceEntry(
                    url=url, title="[DEAD LINK]", status="dead_link",
                    http_code=404, final_url=response.url,
                )
            if response.status_code == 410:
                return SourceEntry(
                    url=url, title="[GONE]", status="dead_link",
                    http_code=410, final_url=response.url,
                )
            if response.status_code == 403:
                return SourceEntry(
                    url=url, title="[ACCESS DENIED]", status="blocked",
                    http_code=403, final_url=response.url,
                )
            if response.status_code == 451:
                return SourceEntry(
                    url=url, title="[UNAVAILABLE FOR LEGAL REASONS]",
                    status="blocked", http_code=451, final_url=response.url,
                )
            if response.status_code == 429:
                wait = RETRY_DELAY * (attempt + 1)
                logger.debug(f"429 for {url}, waiting {wait}s...")
                time.sleep(wait)
                continue
            if response.status_code >= 500:
                logger.debug(
                    f"{response.status_code} for {url}, "
                    f"retry {attempt + 1}/{MAX_RETRIES}"
                )
                time.sleep(RETRY_DELAY)
                last_error = f"HTTP {response.status_code}"
                continue
            if response.status_code != 200:
                return SourceEntry(
                    url=url, title=f"[HTTP {response.status_code}]",
                    status="error", http_code=response.status_code,
                    final_url=response.url,
                )

            html_text = None
            encodings_to_try = []

            encodings_to_try.append("utf-8")

            if response.encoding and response.encoding.lower() != "utf-8":
                encodings_to_try.append(response.encoding)

            for encoding in encodings_to_try:
                try:
                    html_text = body.decode(encoding)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue

            if html_text is None:
                try:
                    apparent = getattr(response, "apparent_encoding", None)
                    if apparent:
                        html_text = body.decode(apparent)
                except Exception:
                    pass

            if html_text is None:
                html_text = body.decode("utf-8", errors="replace")
                logger.debug(f"Forced utf-8 decode with replacement for {url}")

            title = _extract_title_from_html(html_text)
            return SourceEntry(
                url=url,
                title=title,
                status="ok",
                http_code=200,
                content_type=content_type.split(";")[0].strip(),
                final_url=response.url,
            )

        except requests.exceptions.TooManyRedirects:
            return SourceEntry(
                url=url, title="[REDIRECT LOOP]", status="redirect_loop",
                error_detail="Too many redirects",
            )
        except requests.exceptions.SSLError as e:
            return SourceEntry(
                url=url, title="[SSL ERROR]", status="error",
                error_detail=str(e)[:100],
            )
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                last_error = "Timeout"
                continue
            return SourceEntry(
                url=url, title="[TIMEOUT]", status="timeout",
                error_detail=f"Timeout after {REQUEST_TIMEOUT}s",
            )
        except requests.exceptions.ConnectionError as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                last_error = f"Connection error: {str(e)[:80]}"
                continue
            return SourceEntry(
                url=url, title="[CONNECTION ERROR]", status="connection_error",
                error_detail=str(e)[:100],
            )
        except requests.exceptions.RequestException as e:
            return SourceEntry(
                url=url, title="[REQUEST ERROR]", status="error",
                error_detail=str(e)[:100],
            )
        except Exception as e:
            return SourceEntry(
                url=url, title="[CRITICAL PARSE ERROR]", status="error",
                error_detail=str(e)[:100],
            )

    return SourceEntry(
        url=url,
        title=f"[FAILED: {last_error}]",
        status="error",
        error_detail=f"Failed after {MAX_RETRIES} retries: {last_error}",
    )

def load_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    try:
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(cache_path: Path, cache: dict) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except OSError:
        pass

def _safe_write_json(
    filepath: Path, data: dict, logger: logging.Logger
) -> bool:
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(data, ensure_ascii=False, indent=2)
        filepath.write_text(content, encoding="utf-8")
        return True
    except OSError as e:
        logger.error(f"WRITE FAILED: {filepath} - {e}")
        return False

def output_exists(date_str: str) -> bool:
    return (SOURCES_DIR / f"{date_str}.json").exists()

def get_available_html_files() -> list[Path]:
    if not HTML_DIR.exists():
        return []
    return sorted(
        f for f in HTML_DIR.glob("*.html")
        if re.match(r"\d{4}-\d{2}-\d{2}\.html$", f.name)
    )


def _date_from_filename(filepath: Path) -> str:
    return filepath.stem


def process_single_report(
        html_path: Path,
        session: requests.Session | None,
        logger: logging.Logger,
        cache: dict,
        force: bool = False,
        skip_resolve: bool = False,
) -> ReportSources:
    date_str = _date_from_filename(html_path)
    report = ReportSources(report_date=date_str, html_file=html_path.name)

    urls = extract_urls_from_html(html_path, logger)
    report.sources_count = len(urls)

    if not urls:
        logger.debug(f"{date_str}: No external URLs found")
        return report

    def fetch_url_data(url):
        cachekey = url
        if cachekey in cache and not force:
            entry_raw = cache[cachekey]
            return SourceEntry(
                url=entry_raw["url"],
                title=entry_raw["title"],
                status=entry_raw["status"],
                http_code=entry_raw.get("http_code", 0),
                content_type=entry_raw.get("content_type", ""),
                final_url=entry_raw.get("final_url", ""),
                error_detail=entry_raw.get("error_detail", ""),
            )

        stitle, st_status = _special_title(url)
        if stitle:
            return SourceEntry(url=url, title=stitle, status=st_status)

        if not skip_resolve and session is not None:
            entry = resolve_source_title(url, session, logger)
        else:
            entry = SourceEntry(url=url, title="[UNRESOLVED]", status="unresolved")

        return entry

    with ThreadPoolExecutor(max_workers=15) as executor:
        future_to_url = {executor.submit(fetch_url_data, url): url for url in urls}

        for future in as_completed(future_to_url):
            try:
                entry = future.result()

                cache[entry.url] = entry.to_dict()
                report.sources.append(entry)

                if entry.status == "ok":
                    report.resolved_count += 1
                elif entry.status in ("dead_link", "timeout", "connection_error", "error"):
                    report.dead_count += 1
                elif entry.status == "blocked":
                    report.blocked_count += 1
                elif entry.status == "non_html":
                    report.non_html_count += 1

            except Exception as exc:
                logger.error(f"URL {future_to_url[future]} generated an exception: {exc}")

    logger.info(
        f"{date_str}: {report.sources_count} sources | "
        f"ok={report.resolved_count} dead={report.dead_count} "
        f"blocked={report.blocked_count} non_html={report.non_html_count}"
    )
    return report

def process_reports(
    html_files: list[Path],
    logger: logging.Logger,
    force: bool = False,
    skip_resolve: bool = False,
) -> ProcessStats:
    total = len(html_files)
    stats = ProcessStats(total_reports=total)
    failed_dates: list[str] = []
    is_multi = total > 1

    logger.info(f"Processing {total} HTML files...")

    iterator = tqdm(
        html_files, desc="Extracting sources", unit="report",
        disable=(not is_multi),
    )

    session = None
    if not skip_resolve:
        session = requests.Session()
        session.headers.update(HEADERS)
        session.max_redirects = MAX_REDIRECTS

    cache = load_cache(CACHE_PATH)
    last_cache_save = 0

    try:
        for i, html_path in enumerate(iterator):
            date_str = _date_from_filename(html_path)

            if not force and output_exists(date_str):
                logger.debug(f"SKIP: {date_str} - already exists")
                stats.skipped += 1
                if is_multi:
                    iterator.set_postfix(
                        done=stats.processed, skip=stats.skipped
                    )
                continue

            report = process_single_report(
                html_path, session, logger, cache,
                force=force, skip_resolve=skip_resolve,
            )

            output_path = SOURCES_DIR / f"{date_str}.json"
            success = _safe_write_json(output_path, report.to_dict(), logger)

            if success:
                stats.processed += 1
                stats.total_sources += report.sources_count
                stats.resolved += report.resolved_count
                stats.dead += report.dead_count
                stats.blocked += report.blocked_count
                stats.non_html += report.non_html_count
            else:
                stats.errors += 1
                failed_dates.append(date_str)

            if is_multi:
                iterator.set_postfix(
                    done=stats.processed,
                    skip=stats.skipped,
                    src=stats.total_sources,
                )

            if (i + 1) % CACHE_SAVE_EVERY == 0:
                save_cache(CACHE_PATH, cache)
                last_cache_save = i + 1

    finally:
        if session is not None:
            session.close()
        if last_cache_save != len(html_files):
            save_cache(CACHE_PATH, cache)

    _log_summary(logger, stats, failed_dates, use_tqdm=is_multi)
    return stats

def show_stats(logger: logging.Logger) -> None:
    if not SOURCES_DIR.exists():
        print("No source files found. Run with --all first.")
        return

    json_files = sorted(SOURCES_DIR.glob("*.json"))
    if not json_files:
        print("No source files found. Run with --all first.")
        return

    total_files = len(json_files)
    total_sources = 0
    total_resolved = 0
    total_dead = 0
    total_blocked = 0
    total_non_html = 0
    status_counts: dict[str, int] = {}
    domain_counts: dict[str, int] = {}

    for json_file in json_files:
        try:
            data = json.loads(
                json_file.read_text(encoding="utf-8", errors="replace")
            )
            for source in data.get("sources", []):
                total_sources += 1
                status = source.get("status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1

                if status == "ok":
                    total_resolved += 1
                elif status in (
                    "dead_link", "timeout", "connection_error", "error"
                ):
                    total_dead += 1
                elif status == "blocked":
                    total_blocked += 1
                elif status == "non_html":
                    total_non_html += 1

                domain = _get_domain(source.get("url", ""))
                if domain:
                    domain_counts[domain] = domain_counts.get(domain, 0) + 1

        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Error reading {json_file.name}: {e}")

    print("")
    print("=" * 65)
    print("  ISW SOURCES - STATISTICS")
    print("=" * 65)
    print(f"  Reports (JSON):   {total_files}")
    print(f"  Total sources:    {total_sources}")
    print(f"  Resolved (ok):    {total_resolved}")
    print(f"  Dead links:       {total_dead}")
    print(f"  Blocked:          {total_blocked}")
    print(f"  Non-HTML:         {total_non_html}")
    print()
    print("  Status breakdown:")
    for status, count in sorted(
        status_counts.items(), key=lambda x: -x[1]
    ):
        pct = count / total_sources * 100 if total_sources else 0
        print(f"    {status:20s} {count:6d} ({pct:5.1f}%)")
    print()
    top_domains = sorted(domain_counts.items(), key=lambda x: -x[1])[:20]
    print("  Top 20 source domains:")
    for domain, count in top_domains:
        print(f"    {domain:40s} {count:5d}")
    print("=" * 65)

def _log_summary(
    logger: logging.Logger,
    stats: ProcessStats,
    failed_dates: list[str],
    use_tqdm: bool = False,
) -> None:
    lines = [
        "",
        "=" * 65,
        "  SOURCES EXTRACTION COMPLETE",
        "=" * 65,
        f"  Total reports:      {stats.total_reports}",
        f"  Processed:          {stats.processed}",
        f"  Skipped (existed):  {stats.skipped}",
        f"  Write errors:       {stats.errors}",
        "",
        f"  Total sources:      {stats.total_sources}",
        f"  Resolved (title):   {stats.resolved}",
        f"  Dead/error:         {stats.dead}",
        f"  Blocked:            {stats.blocked}",
        f"  Non-HTML:           {stats.non_html}",
        "=" * 65,
    ]

    if failed_dates:
        shown = failed_dates[:20]
        lines.append(f"  Failed dates ({len(failed_dates)}):")
        lines.extend([f"    - {d}" for d in shown])
        if len(failed_dates) > 20:
            lines.append(f"  ... and {len(failed_dates) - 20} more.")

    for line in lines:
        logger.info(line)
    if use_tqdm:
        for line in lines:
            tqdm.write(line)

def valid_date(date_string: str) -> str:
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return date_string
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_string}'. Use YYYY-MM-DD."
        )

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "ISW Sources Scraper — extract external source references "
            "from locally saved ISW HTML reports"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrapers/isw_sources_scraper.py --all
  python scrapers/isw_sources_scraper.py --date 2024-06-15
  python scrapers/isw_sources_scraper.py --all --force
  python scrapers/isw_sources_scraper.py --all --skip-resolve
  python scrapers/isw_sources_scraper.py --stats
        """,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--all", action="store_true",
        help="Process all HTML files in data/raw/isw_reports/html/",
    )
    mode.add_argument(
        "--date", type=valid_date,
        help="Process specific date (YYYY-MM-DD)",
    )
    mode.add_argument(
        "--stats", action="store_true",
        help="Show statistics about existing source files",
    )

    parser.add_argument(
        "--force", action="store_true",
        help="Re-process even if JSON already exists",
    )
    parser.add_argument(
        "--skip-resolve", action="store_true",
        help="Extract URLs only, don't resolve titles via HTTP",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose console output",
    )

    return parser.parse_args()

def main() -> None:
    args = parse_arguments()

    if args.stats:
        logger = setup_logging(verbose=True)
        show_stats(logger)
        return

    verbose = getattr(args, "verbose", False)
    is_single = bool(getattr(args, "date", None))
    logger = setup_logging(verbose=(verbose or is_single))

    logger.info("=" * 65)
    logger.info("ISW SOURCES SCRAPER — STARTING")
    logger.info(f"BS4 parser:  {BS4_PARSER}")
    logger.info(f"HTML dir:    {HTML_DIR}")
    logger.info(f"Output dir:  {SOURCES_DIR}")
    logger.info(f"Cache:       {CACHE_PATH}")
    if args.skip_resolve:
        logger.info("Mode: URL extraction only (no HTTP to external sources)")
    logger.info("=" * 65)

    SOURCES_DIR.mkdir(parents=True, exist_ok=True)

    if not HTML_DIR.exists():
        logger.error(
            f"HTML directory not found: {HTML_DIR}\n"
            f"Run isw_scraper.py --backfill first to download ISW reports."
        )
        sys.exit(1)

    start_time = time.time()

    if args.date:
        html_path = HTML_DIR / f"{args.date}.html"
        if not html_path.exists():
            logger.error(
                f"HTML file not found: {html_path}\n"
                f"Run: python scrapers/isw_scraper.py --date {args.date}"
            )
            sys.exit(1)
        logger.info(f"Mode: SINGLE DATE ({args.date})")
        stats = process_reports(
            [html_path], logger,
            force=args.force, skip_resolve=args.skip_resolve,
        )

    elif args.all:
        html_files = get_available_html_files()
        if not html_files:
            logger.error(
                f"No HTML files found in {HTML_DIR}\n"
                f"Run isw_scraper.py --backfill first."
            )
            sys.exit(1)

        logger.info(f"Mode: ALL ({len(html_files)} HTML files)")
        if args.force:
            logger.info("Force mode: will re-process existing files")

        stats = process_reports(
            html_files, logger,
            force=args.force, skip_resolve=args.skip_resolve,
        )

    elapsed = time.time() - start_time
    json_count = len(list(SOURCES_DIR.glob("*.json")))

    logger.info(
        f"Done in {elapsed:.1f}s ({elapsed / 60:.1f} min) | "
        f"Processed: {stats.processed} | Skipped: {stats.skipped} | "
        f"Sources: {stats.total_sources} | "
        f"JSON files on disk: {json_count}"
    )

if __name__ == "__main__":
    main()