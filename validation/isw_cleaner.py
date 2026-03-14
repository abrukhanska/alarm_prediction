import argparse
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import scipy.sparse
from sklearn.feature_extraction.text import TfidfVectorizer

PROJECT_ROOT    = Path(__file__).resolve().parent.parent
ISW_TEXT_DIR    = PROJECT_ROOT / "data" / "raw" / "isw_reports" / "text"
ISW_SOURCES_DIR = PROJECT_ROOT / "data" / "raw" / "isw_sources"
PROCESSED_DIR   = PROJECT_ROOT / "data" / "processed"

OUTPUT_CSV   = PROCESSED_DIR / "isw_clean.csv"
OUTPUT_TEXTS = PROCESSED_DIR / "isw_texts.json"
TFIDF_MATRIX = PROCESSED_DIR / "isw_tfidf_matrix.npz"
TFIDF_VOCAB  = PROCESSED_DIR / "isw_tfidf_vocab.json"
REPORT_TXT   = PROCESSED_DIR / "isw_processing_report.txt"

WAR_START         = pd.Timestamp("2022-02-24")
DATA_CUTOFF       = pd.Timestamp.today().normalize()
MIN_REPORT_LENGTH = 200
MAX_REPORT_LENGTH = 200_000

ATTACK_WORDS = frozenset({
    "strike", "strikes", "struck", "missile", "missiles",
    "drone", "drones", "shahed", "kalibr", "iskander", "khinzhal",
    "attack", "attacked", "attacks", "bombing", "bombardment",
    "shelling", "shelled", "artillery", "launch", "launched",
    "intercept", "intercepted", "explosion", "explosions",
})

GROUND_WORDS = frozenset({
    "advance", "advanced", "advances", "advancing",
    "assault", "assaulted", "assaults",
    "counterattack", "counterattacked", "counterattacks",
    "offensive", "captured", "liberated", "recaptured",
    "push", "pushed", "gain", "gained", "gains",
    "retreat", "retreated", "withdrew", "withdrawal",
    "encircle", "encircled", "flank", "flanked",
})

CASUALTY_WORDS = frozenset({
    "casualties", "killed", "wounded", "destroyed", "losses",
    "eliminated", "neutralized", "damaged", "hit",
    "dead", "injured", "annihilated",
})

# EDA only — fit on full dataset is intentional here.
# isw_nlp_pipeline.py uses 500 features fit on TRAIN only.
TFIDF_MAX_FEATURES = 5000
TFIDF_MIN_DF       = 5
TFIDF_MAX_DF       = 0.95

def _normalize_weapons(text: str) -> str:
    return re.sub(r'(?<=[a-zA-Z0-9])-(?=[0-9])', '', text)

def _count_keywords(text: str, keywords: frozenset) -> int:
    clean_text = _normalize_weapons(text.lower())
    words = re.findall(r'\b[a-zA-Z0-9]{3,}\b', clean_text)
    return sum(1 for w in words if w in keywords)

def load_texts() -> tuple[pd.DataFrame, dict[str, str], list[str]]:
    records, texts, issues = [], {}, []

    if not ISW_TEXT_DIR.exists():
        print(f"ERROR: {ISW_TEXT_DIR} not found")
        print("  Run: python scrapers/isw_scraper.py --backfill")
        sys.exit(1)

    json_files = sorted(ISW_TEXT_DIR.glob("*.json"))
    txt_files  = sorted(ISW_TEXT_DIR.glob("*.txt"))
    all_files  = json_files + txt_files

    if not all_files:
        print(f"ERROR: no files in {ISW_TEXT_DIR}")
        sys.exit(1)

    print(f"Found {len(json_files)} JSON + {len(txt_files)} TXT files")

    for fp in all_files:
        date_str = fp.stem

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            issues.append(f"FILENAME: bad format '{fp.name}'")
            continue
        try:
            date = pd.Timestamp(date_str)
        except Exception:
            issues.append(f"FILENAME: invalid date '{date_str}'")
            continue
        if date < WAR_START:
            issues.append(f"DATE: {date_str} before war start")
            continue
        if date > DATA_CUTOFF:
            issues.append(f"DATE: {date_str} after cutoff")
            continue

        try:
            raw = fp.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                raw = fp.read_text(encoding="latin-1")
                issues.append(f"ENCODING: {fp.name} - latin-1 fallback")
            except Exception as e:
                issues.append(f"READ: {fp.name} — {e}")
                continue
        except Exception as e:
            issues.append(f"READ: {fp.name} — {e}")
            continue

        text, url = "", ""
        if fp.suffix == ".json":
            try:
                data   = json.loads(raw)
                text   = data.get("text", "")
                url    = data.get("url", "")
                status = data.get("status", "unknown")
                if status != "ok":
                    issues.append(f"STATUS: {date_str} — '{status}'")
            except json.JSONDecodeError as e:
                issues.append(f"JSON: {fp.name} — {e}")
                continue
        else:
            text = raw

        text = text.strip()

        if not text:
            issues.append(f"EMPTY: {fp.name}")
            continue
        if len(text) < MIN_REPORT_LENGTH:
            issues.append(f"SHORT: {date_str} — {len(text)} chars")
            continue
        if len(text) > MAX_REPORT_LENGTH:
            issues.append(f"HUGE: {date_str} — {len(text):,} chars — trimmed")
            text = text[:MAX_REPORT_LENGTH]

        sample_words = re.findall(r'\b\w+\b', text[:1000].lower())[:100]
        en_markers = {
            "the", "and", "of", "in", "to", "for", "is", "on", "that", "with",
            "forces", "russian", "ukrainian", "reported", "according", "attack",
            "military", "region", "offensive", "defense", "continued", "units",
        }
        en_count = sum(1 for w in sample_words if w in en_markers)
        if en_count < 5:
            issues.append(f"LANGUAGE: {date_str} - only {en_count}/30 EN markers")

        word_count      = len(text.split())
        sentence_count  = max(len(re.split(r"[.!?]+", text)), 1)
        paragraph_count = max(len([p for p in text.split("\n\n") if p.strip()]), 1)

        records.append({
            "date":                date,
            "isw_report_length":   len(text),
            "word_count":          word_count,
            "sentence_count":      sentence_count,
            "paragraph_count":     paragraph_count,
            "avg_sentence_length": round(word_count / sentence_count, 1),
            "url":                 url,
        })
        texts[date_str] = text

    if not records:
        print("ERROR: no valid records loaded")
        sys.exit(1)

    df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)

    before = len(df)
    df = (df.sort_values(["date", "isw_report_length"], ascending=[True, False])
            .drop_duplicates(subset=["date"], keep="first")
            .reset_index(drop=True))
    if len(df) < before:
        issues.append(f"DUPLICATES: removed {before - len(df)}")
    return df, texts, issues

def load_sources(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    issues  = []
    src_cols = ["isw_sources_count", "sources_resolved",
                "sources_dead", "sources_blocked", "unique_domains"]
    if not ISW_SOURCES_DIR.exists() or not list(ISW_SOURCES_DIR.glob("*.json")):
        issues.append("SOURCES: dir not found or empty — filling with 0")
        for col in src_cols:
            df[col] = 0
        return df, issues

    source_files = sorted(ISW_SOURCES_DIR.glob("*.json"))
    print(f"  Found {len(source_files)} source files")
    records = []

    for fp in source_files:
        try:
            data     = json.loads(fp.read_text(encoding="utf-8"))
            date_str = data.get("report_date", fp.stem)
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
                continue

            sources_list = data.get("sources", [])
            domains = set()
            for src in sources_list:
                try:
                    d = urlparse(src.get("url", "")).netloc.lower()
                    if d:
                        domains.add(d)
                except Exception as e:
                    issues.append(f"SOURCE URL parse error in {fp.name}: {e}")

            records.append({
                "date":              pd.Timestamp(date_str),
                "isw_sources_count": data.get("sources_count", len(sources_list)),
                "sources_resolved":  data.get("resolved_count", 0),
                "sources_dead":      data.get("dead_count", 0),
                "sources_blocked":   data.get("blocked_count", 0),
                "unique_domains":    len(domains),
            })
        except Exception as e:
            issues.append(f"SOURCE: {fp.name} — {e}")

    if records:
        df = df.merge(pd.DataFrame(records), on="date", how="left")
        for col in src_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        zero = (df["isw_sources_count"] == 0).sum()
        if zero:
            issues.append(f"SOURCES: {zero} reports with 0 sources")
    else:
        for col in src_cols:
            df[col] = 0

    return df, issues

def check_gaps(df: pd.DataFrame) -> list[str]:
    issues   = []
    all_d    = pd.date_range(df.date.min(), df.date.max(), freq="D")
    missing  = sorted(set(all_d) - set(df.date))
    coverage = (1 - len(missing) / len(all_d)) * 100

    issues.append(f"COVERAGE: {coverage:.1f}% ({len(df)}/{len(all_d)} days)")

    if not missing:
        return issues

    gaps, start, end = [], missing[0], missing[0]
    for i in range(1, len(missing)):
        if missing[i] - missing[i - 1] == pd.Timedelta(days=1):
            end = missing[i]
        else:
            gaps.append((start, end, (end - start).days + 1))
            start = end = missing[i]
    gaps.append((start, end, (end - start).days + 1))

    issues.append(f"GAPS: {len(missing)} missing dates in {len(gaps)} gap(s)")
    for s, e, n in gaps[:10]:
        issues.append(f"  GAP: {s.date()} → {e.date()} ({n} day{'s' if n > 1 else ''})")
    if len(gaps) > 10:
        issues.append(f"  ... +{len(gaps) - 10} more")

    return issues

def extract_keywords(df: pd.DataFrame, texts: dict) -> pd.DataFrame:
    attack, ground, casualty = [], [], []

    for _, row in df.iterrows():
        text = texts.get(row["date"].strftime("%Y-%m-%d"), "")
        attack.append(_count_keywords(text, ATTACK_WORDS))
        ground.append(_count_keywords(text, GROUND_WORDS))
        casualty.append(_count_keywords(text, CASUALTY_WORDS))

    df["attack_mentions"]    = attack
    df["ground_mentions"]    = ground
    df["casualty_mentions"]  = casualty
    df["total_intensity"]    = df["attack_mentions"] + df["ground_mentions"] + df["casualty_mentions"]
    df["intensity_per_1000"] = (df["total_intensity"] / df["isw_report_length"] * 1000).round(2)

    return df

def build_tfidf(df: pd.DataFrame, texts: dict):
    corpus = [texts.get(d.strftime("%Y-%m-%d"), "") for d in df.date]

    vectorizer = TfidfVectorizer(
        max_features=TFIDF_MAX_FEATURES,
        stop_words="english",
        min_df=TFIDF_MIN_DF,
        max_df=TFIDF_MAX_DF,
        ngram_range=(1, 2),
        token_pattern=r"(?u)\b[a-zA-Z]{3,}\b",
        sublinear_tf=True,
    )

    matrix = vectorizer.fit_transform(corpus)
    vocab  = vectorizer.get_feature_names_out().tolist()
    return matrix, vocab

def process():
    all_issues = []

    print("\n" + "=" * 65)
    print("  STEP 1/6: Loading ISW report texts")
    print("=" * 65)
    df, texts, issues = load_texts()
    all_issues.extend(issues)
    print(f"  Loaded:  {len(df):,} reports")
    print(f"  Range:   {df.date.min().date()} → {df.date.max().date()}")
    print(f"  Issues:  {len(issues)}")

    print("\n" + "=" * 65)
    print("  STEP 2/6: Loading ISW sources")
    print("=" * 65)
    df, issues = load_sources(df)
    all_issues.extend(issues)
    print(f"  Sources avg: {df.isw_sources_count.mean():.1f}")
    print(f"  Issues:      {len(issues)}")

    print("\n" + "=" * 65)
    print("  STEP 3/6: Date gap analysis")
    print("=" * 65)
    issues = check_gaps(df)
    all_issues.extend(issues)
    for g in issues[:5]:
        print(f"  {g}")

    print("\n" + "=" * 65)
    print("  STEP 4/6: Keyword features")
    print("=" * 65)
    df = extract_keywords(df, texts)
    print(f"  attack avg:   {df.attack_mentions.mean():.1f}")
    print(f"  ground avg:   {df.ground_mentions.mean():.1f}")
    print(f"  casualty avg: {df.casualty_mentions.mean():.1f}")
    print(f"  total avg:    {df.total_intensity.mean():.1f}")

    print("\n" + "=" * 65)
    print("  STEP 5/6: TF-IDF matrix (EDA only — fit on full dataset)")
    print("=" * 65)
    tfidf_matrix, vocab = build_tfidf(df, texts)
    print(f"  Matrix: {tfidf_matrix.shape[0]:,} × {tfidf_matrix.shape[1]:,}")
    mean_w = np.asarray(tfidf_matrix.mean(axis=0)).flatten()
    top5   = [vocab[i] for i in mean_w.argsort()[::-1][:5]]
    print(f"  Top-5:  {top5}")

    print("\n" + "=" * 65)
    print("  STEP 6/6: Validation & Save")
    print("=" * 65)

    feature_cols = [
        "date", "isw_report_length", "word_count", "sentence_count",
        "paragraph_count", "avg_sentence_length",
        "isw_sources_count", "sources_resolved", "sources_dead",
        "sources_blocked", "unique_domains",
        "attack_mentions", "ground_mentions", "casualty_mentions",
        "total_intensity", "intensity_per_1000",
    ]
    available = [c for c in feature_cols if c in df.columns]

    for col in available:
        if col == "date":
            continue
        nans = df[col].isna().sum()
        if nans > 0:
            all_issues.append(f"NaN: {col} — {nans}")
            df[col] = df[col].fillna(0)

    for col in available:
        if col == "date":
            continue
        if df[col].dtype == "object":
            all_issues.append(f"DTYPE: {col} is object")
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    assert df.date.is_monotonic_increasing, "Dates not sorted!"

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df_save = df[available].copy()
    df_save.to_csv(OUTPUT_CSV, index=False)
    print(f"  saved {OUTPUT_CSV}  ({df_save.shape[0]:,} × {df_save.shape[1]})")

    with open(OUTPUT_TEXTS, "w", encoding="utf-8") as f:
        json.dump(texts, f, ensure_ascii=False)
    print(f"  saved {OUTPUT_TEXTS}  ({len(texts):,} texts)")

    scipy.sparse.save_npz(TFIDF_MATRIX, tfidf_matrix)
    print(f"  saved {TFIDF_MATRIX}  {tfidf_matrix.shape}")

    with open(TFIDF_VOCAB, "w", encoding="utf-8") as f:
        json.dump(vocab, f, ensure_ascii=False)
    print(f"  saved {TFIDF_VOCAB}  ({len(vocab):,} terms)")

    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("ISW PROCESSING REPORT\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Generated:   {pd.Timestamp.now()}\n")
        f.write(f"Reports:     {len(df):,}\n")
        f.write(f"Date range:  {df.date.min().date()} → {df.date.max().date()}\n")
        f.write(f"Columns:     {len(available)}\n")
        f.write(f"TF-IDF:      {tfidf_matrix.shape}\n")
        f.write(f"Issues:      {len(all_issues)}\n\n")
        f.write("Columns:\n")
        for i, col in enumerate(available, 1):
            nans = df_save[col].isna().sum()
            f.write(f"  {i:>2}. {col:30s} {str(df_save[col].dtype):15s} NaN: {nans}\n")
        f.write(f"\nStats:\n{df_save.describe().round(2).to_string()}\n")
        f.write(f"\nIssues ({len(all_issues)}):\n")
        for i, issue in enumerate(all_issues, 1):
            f.write(f"  {i:>3}. {issue}\n")
    print(f"  saved {REPORT_TXT}")

    print("\n" + "=" * 65)
    print("  ISW PROCESSING — COMPLETE")
    print("=" * 65)
    print(f"  Reports:    {len(df):,}")
    print(f"  Range:      {df.date.min().date()} → {df.date.max().date()}")
    print(f"  Columns:    {len(available)}")
    print(f"  TF-IDF:     {tfidf_matrix.shape}")
    print(f"  Issues:     {len(all_issues)}")
    for col in available:
        if col == "date":
            continue
        print(f"    {col:30s}: mean={df[col].mean():>10.1f}  "
              f"min={df[col].min():>8.0f}  max={df[col].max():>8.0f}")
    if all_issues:
        print(f"\n  Issues (first 15):")
        for i, issue in enumerate(all_issues[:15], 1):
            print(f"    {i:>3}. {issue}")
        if len(all_issues) > 15:
            print(f"    ... +{len(all_issues) - 15} more")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ISW Data Processor")
    parser.add_argument("--process", action="store_true",
                        help="Process raw ISW data into clean features")
    args = parser.parse_args()
    if args.process:
        process()
    else:
        print("Use --process to start.")
        parser.print_help()