import json
import re
import sys
import time
import argparse
import logging
import os
import random
import tempfile
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo
try:
    import cloudscraper
except ImportError:
    print("ERROR: pip install cloudscraper")
    sys.exit(1)
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "gur"
LOG_FILE = PROJECT_ROOT / "logs" / "gur_scraper.log"
KYIV_TZ = ZoneInfo("Europe/Kyiv")
BASE_URL = "https://gur.gov.ua/ua/content/list-of-news.html"

KW_MASSIVE_SUCCESS = [
    r"збит", r"знищ", r"перехопл", r"нейтраліз", r"ліквід", r"потопл", r"ураж",
    r"розбит", r"успішн.*операці", r"вибух", r"зачищ", r"відбит", r"спецопераці", r"звільнен",
    r"прорив", r"бавовн", r"демілітариз", r"червон.*площ", r"кремл"
]
KW_RETALIATION = [
    r"втрат.*противник", r"знищен.*сил", r"командир", r"генерал", r"полковник", r"підрозділ.*знищ",
    r"бригад.*розгром", r"окупант.*втрат", r"ліквідаці", r"вбит", r"загинул.*окупант",
    r"втрат.*ворог", r"мертв", r"полон", r"герасимов", r"шойгу", r"суровікін", r"лапін", r"мізинцев",
    r"кадиров", r"вагнер", r"спн", r"гру", r"фсб", r"росгварді"
]
KW_LOGISTICS = [
    r"склад", r"баз.*постачан", r"залізни", r"палив", r"боєприпас", r"логістик", r"завод",
    r"виробництв", r"нафтобаз", r"аеродром", r"корабел", r"бпла", r"дрон", r"міст",
    r"ешелон", r"військов.*технік", r"авіабаз", r"рсзв", r"іскандер", r"калібр", r"кинжал",
    r"с-300", r"с-400", r"бук", r"тор", r"панцир", r"енгельс"
]
KW_ENERGY = [
    r"електростанці", r"підстанці", r"тепловий вузол", r"енергетичн", r"блекаут", r"знеструмлен",
    r"інфраструктур", r"трансформатор", r"гес", r"тец", r"заес", r"курськ.*аес", r"росєнєрго"
]
KW_INTERCEPTS = [
    r"перехоплен", r"зв'язок.*противник", r"радіообмін", r"сигнал.*розвідк", r"агент", r"шпигун",
    r"джерело.*ворог", r"розмов.*окупант", r"аудіо", r"розвіддан", r"перехоплен.*розмов",
    r"гур.*здобул", r"інсайдер", r"перехват", r"раци", r"телефон.*разговор"
]

def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("gur_scraper")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger

def _count_keywords(text: str, keywords: list) -> int:
    text_lower = text.lower()
    return sum(1 for kw in keywords if re.search(kw, text_lower))

def fetch_html(scraper, url, logger, retries=3):
    for attempt in range(retries):
        try:
            resp = scraper.get(url, timeout=20)
            if resp.status_code == 200:
                return resp.text
            time.sleep(5)
        except Exception:
            time.sleep(5)
    logger.error(f"Cannot load page: {url}")
    return None

def parse_article(scraper, article_el, logger):
    date_el = article_el.select_one("time, span.date-display-single, span.date, .field-date, .date")
    date_str = date_el.get("datetime") or date_el.get_text(strip=True) if date_el else ""
    if not date_str:
        match = re.search(r'\d{2}\.\d{2}\.\d{4}', article_el.get_text())
        if match:
            date_str = match.group(0)
    if not date_str:
        return None
    try:
        if "." in date_str:
            art_date = datetime.strptime(date_str[:10], "%d.%m.%Y").date()
        else:
            art_date = datetime.fromisoformat(date_str[:10]).date()
    except ValueError:
        return None
    title_el = article_el.select_one("h2 a, h3 a, .views-field-title a, span.title, h2, h3")
    title = title_el.get_text(strip=True) if title_el else article_el.get_text(strip=True)
    if article_el.name == "a":
        href = article_el.get("href", "")
    else:
        link_tag = title_el if (title_el and title_el.name == "a") else article_el.select_one("a")
        href = link_tag.get("href", "") if link_tag else ""
    url_full = f"https://gur.gov.ua{href}" if href.startswith("/") else href
    full_text = title
    text_preview = title
    if url_full:
        time.sleep(random.uniform(0.5, 1.5))
        article_html = fetch_html(scraper, url_full, logger, retries=2)
        if article_html:
            article_soup = BeautifulSoup(article_html, "html.parser")
            body_el = article_soup.select_one(".field-name-body, .field-body, .text-content, article")
            if body_el:
                text_preview = body_el.get_text(separator=" ", strip=True)[:400]
                full_text = f"{title} {body_el.get_text(separator=' ', strip=True)}"
    return {
        "date": str(art_date),
        "art_date": art_date,
        "title": title,
        "url": url_full,
        "text_preview": text_preview,
        "kw_success": _count_keywords(full_text, KW_MASSIVE_SUCCESS),
        "kw_retaliation": _count_keywords(full_text, KW_RETALIATION),
        "kw_logistics": _count_keywords(full_text, KW_LOGISTICS),
        "kw_energy": _count_keywords(full_text, KW_ENERGY),
        "kw_intercepts": _count_keywords(full_text, KW_INTERCEPTS)
    }

def scrape_day(target_date: date, scraper, logger) -> list:
    articles = []
    for page in range(1, 10):
        url = BASE_URL if page == 1 else f"https://gur.gov.ua/ua/content/list-of-news/{page - 1}.html"
        html = fetch_html(scraper, url, logger)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        elements = soup.select("article, div.views-row, div.post, a.item, a.main-item, .news_item")
        if not elements:
            break
        found_older = False
        for el in elements:
            parsed = parse_article(scraper, el, logger)
            if not parsed:
                continue
            if parsed["art_date"] == target_date:
                del parsed["art_date"]
                articles.append(parsed)
            elif parsed["art_date"] < target_date:
                found_older = True
        if found_older:
            break
        time.sleep(random.uniform(1.0, 2.0))
    return articles

def run_backfill(logger):
    scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
    stop_date = date(2022, 2, 24)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting backfill from 2022-02-24")
    total_saved = 0
    for page in range(1, 3000):
        url = BASE_URL if page == 1 else f"https://gur.gov.ua/ua/content/list-of-news/{page - 1}.html"
        html = fetch_html(scraper, url, logger)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        elements = soup.select("article, div.views-row, div.post, a.item, a.main-item, .news_item")
        if not elements:
            logger.info("No more news available.")
            break
        logger.info(f"Reading page {page} (found {len(elements)} articles)")
        daily_data = {}
        found_older_than_2022 = False
        for el in elements:
            parsed = parse_article(scraper, el, logger)
            if not parsed:
                continue
            art_date = parsed["art_date"]
            if art_date < stop_date:
                found_older_than_2022 = True
                continue
            del parsed["art_date"]
            daily_data.setdefault(art_date, []).append(parsed)
        for d, items in daily_data.items():
            out_file = RAW_DIR / f"{d}.json"
            existing_items = []
            if out_file.exists():
                try:
                    with open(out_file, "r", encoding="utf-8") as f:
                        existing_items = json.load(f).get("articles", [])
                except json.JSONDecodeError:
                    pass
            existing_urls = {i["url"] for i in existing_items}
            for it in items:
                if it["url"] not in existing_urls:
                    existing_items.append(it)
                    total_saved += 1
            temp_path = None
            try:
                fd, temp_path = tempfile.mkstemp(dir=RAW_DIR, text=True)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump({"date": str(d), "articles": existing_items, "count": len(existing_items)},
                              f, ensure_ascii=False, indent=2)
                os.replace(temp_path, out_file)
                temp_path = None
            except Exception as e:
                logger.error(f"Failed to save {out_file}: {e}")
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
        if found_older_than_2022:
            logger.info("Reached 2022-02-24. Collection completed.")
            break
        time.sleep(random.uniform(2.0, 4.0))
    logger.info(f"Saved {total_saved} new articles")

def collect_daily(target_date: date, logger) -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_file = RAW_DIR / f"{target_date}.json"
    if out_file.exists():
        logger.info(f"  {target_date}: File already exists - skipping.")
        return 0
    scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
    logger.info(f"Scraping data for {target_date}")
    articles = scrape_day(target_date, scraper, logger)
    if not articles:
        logger.warning(f"  {target_date}: No articles found - writing empty file.")
    temp_path = None
    try:
        fd, temp_path = tempfile.mkstemp(dir=RAW_DIR, text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({
                "date": str(target_date),
                "articles": articles,
                "count": len(articles)
            }, f, ensure_ascii=False, indent=2)
        os.replace(temp_path, out_file)
        temp_path = None
    except Exception as e:
        logger.error(f"Failed to save {out_file}: {e}")
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
    logger.info(f"  {target_date}: Saved {len(articles)} articles to {out_file.name}")
    return len(articles)

def main():
    parser = argparse.ArgumentParser(description="GUR Website Scraper")
    parser.add_argument("--daily", action="store_true", help="Scrape today's news")
    parser.add_argument("--backfill", action="store_true", help="Backfill historical news up to 2022-02-24")
    parser.add_argument("--date", type=str, help="Scrape specific date (Format: YYYY-MM-DD)")
    args = parser.parse_args()
    logger = setup_logging()
    logger.info("--- GUR SCRAPER STARTED ---")
    if args.backfill:
        run_backfill(logger)
        return
    if args.date:
        dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    elif args.daily:
        dates = [datetime.now(KYIV_TZ).date()]
    else:
        parser.print_help()
        return
    total = 0
    for d in dates:
        total += collect_daily(d, logger)
        if len(dates) > 1:
            time.sleep(1.0)
    logger.info(f"--- DONE. Processed {total} articles in total. ---")
if __name__ == "__main__":
    main()