"""
ISW NLP Pipeline
Reads raw texts, filters stop-words/noise, and builds a 500-feature TF-IDF matrix.
Creates alarm_date = D+1 mapping for merge.
"""
import argparse
import json
import re
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import scipy.sparse
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"

INPUT_PARQUET = PROCESSED / "isw_clean.parquet"
INPUT_TEXTS   = PROCESSED / "isw_texts.json"

OUT_TFIDF    = PROCESSED / "tfidf_matrix_model.npz"
OUT_VOCAB    = PROCESSED / "tfidf_vocab_model.json"
OUT_FEATURES = PROCESSED / "isw_features_for_merge.parquet"

TFIDF_FEATURES = 500   # ML model — 500 reduces noise vs EDA processor's 5000

def _normalize_weapons(text: str) -> str:
    return re.sub(r'(?<=[a-zA-Z0-9])-(?=[0-9])', '', text)

def load_processed() -> tuple[pd.DataFrame, list[str]]:
    print("=" * 65)
    print("  STEP 1/3: Load processed ISW data")
    print("=" * 65)

    missing = [p for p in [INPUT_PARQUET, INPUT_TEXTS] if not p.exists()]
    if missing:
        print("ERROR: missing input files — run isw_processor.py --process first")
        for p in missing:
            print(f"    {p}")
        sys.exit(1)

    df = pd.read_parquet(INPUT_PARQUET)

    with open(INPUT_TEXTS, "r", encoding="utf-8") as f:
        texts = json.load(f)

    corpus = [texts.get(d.strftime("%Y-%m-%d"), "") for d in df["date"]]

    print(f"  reports: {len(df):,}")
    print(f"  range:   {df.date.min().date()} -> {df.date.max().date()}")
    print(f"  columns: {df.columns.tolist()}")
    return df, corpus

def build_ml_tfidf(
    df: pd.DataFrame, corpus: list[str]
) -> tuple[scipy.sparse.csr_matrix, list[str]]:
    print("\n" + "=" * 65)
    print("  STEP 2/3: Build ML TF-IDF (fit on TRAIN only)")
    print("=" * 65)

    try:
        alarms_file = PROCESSED / "alarms_clean.parquet"
        df_a = pd.read_parquet(alarms_file)
        start_dt = pd.to_datetime(df_a["start_dt"])
        max_alarm_date = start_dt.dt.tz_localize(None).max().floor("D")
    except Exception as e:
        print(f"Warning: Could not read alarms for sync. Using ISW date. {e}")
        max_alarm_date = df["date"].max().floor("D")
    train_cutoff = max_alarm_date - pd.Timedelta(days=30)

    corpus_norm = [_normalize_weapons(c) for c in corpus]

    train_mask = df["date"] < train_cutoff
    train_texts = [c for c, m in zip(corpus_norm, train_mask) if m]
    n_train = len(train_texts)
    n_total = len(corpus_norm)

    print(f"  train: {n_train} docs (before {train_cutoff.date()})")
    print(f"  test:  {n_total - n_train} docs")
    print(f"  features: {TFIDF_FEATURES}")

    custom_stop_words = list(ENGLISH_STOP_WORDS.union({
        "click", "com", "dot", "html", "http", "https", "www", "org",
        "interactive", "map", "time", "lapse", "isw", "ability", "able",
        "access", "additional", "activities", "activity", "area", "areas",
        "available", "continue", "continued", "continues", "continuing",
        "day", "days", "did", "does", "efforts", "elements", "events",
        "including", "information", "likely", "make", "new", "note",
        "noted", "observed", "ongoing", "past", "percent", "provided",
        "published", "recent", "recently", "report", "reported",
        "reportedly", "reporting", "reports", "shows", "significant",
        "source", "sources", "stated", "support", "today", "january",
        "february", "march", "april", "may", "june", "july", "august",
        "september", "october", "november", "december",
        "000", "1st", "2nd", "3rd", "2022", "2023", "2024", "2025", "2026"
    }))

    vectorizer = TfidfVectorizer(
        max_features=TFIDF_FEATURES, stop_words=custom_stop_words,
        ngram_range=(1, 2), min_df=5, max_df=0.85,
        sublinear_tf=True, token_pattern=r"(?u)\b[a-zA-Z0-9]{3,}\b",
    )
    vectorizer.fit(train_texts)  # fit on TRAIN only — no leakage
    matrix = vectorizer.transform(corpus_norm)  # transform on all (train + test)
    vocab = vectorizer.get_feature_names_out().tolist()

    print(f"  matrix: {matrix.shape}")
    expected = ["attack", "strike", "missile", "drone", "forces",
                "russian", "ukrainian", "artillery", "infantry", "defense"]
    found   = [w for w in expected if w in vocab]
    missing = [w for w in expected if w not in vocab]
    print(f"  war terms in vocab: {found}")
    if missing:
        print(f"  NOT in vocab (too rare / filtered by min_df): {missing}")
    return matrix, vocab, train_cutoff

def build_merge_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 65)
    print("  STEP 3/3: Build merge features with D+1 shift")
    print("=" * 65)

    cols_for_model = [
        "date",
        "isw_report_length", "word_count", "sentence_count", "paragraph_count",
        "avg_sentence_length", "isw_sources_count", "sources_resolved",
        "sources_dead", "sources_blocked", "unique_domains",
        "real_dead_ratio", "blackout_score", "ru_ua_balance", "ru_official_ratio",
        "attack_mentions", "ground_mentions", "casualty_mentions",
        "total_intensity", "intensity_per_1000",
    ]

    available = [c for c in cols_for_model if c in df.columns]
    dropped   = [c for c in cols_for_model if c not in df.columns]
    if dropped:
        print(f"  NOTE: columns not in isw_clean.parquet (skipped): {dropped}")

    df_out = df[available].copy()

    # D+1 shift: ISW report on day D predicts alarms on day D+1
    df_out["alarm_date"] = df_out["date"] + pd.Timedelta(days=1)

    print(f"  ISW date range:   {df_out.date.min().date()} -> {df_out.date.max().date()}")
    print(f"  alarm_date range: {df_out.alarm_date.min().date()} -> {df_out.alarm_date.max().date()}")
    print(f"  features: {[c for c in df_out.columns if c not in ['date', 'alarm_date']]}")
    return df_out

def save_all(
    matrix:      scipy.sparse.csr_matrix,
    vocab:       list[str],
    df_features: pd.DataFrame,
) -> None:
    print("\n" + "=" * 65)
    print("  Save outputs")
    print("=" * 65)
    PROCESSED.mkdir(parents=True, exist_ok=True)

    scipy.sparse.save_npz(OUT_TFIDF, matrix)
    print(f"  saved: {OUT_TFIDF}  {matrix.shape}")

    with open(OUT_VOCAB, "w", encoding="utf-8") as f:
        json.dump(vocab, f, ensure_ascii=False)
    print(f"  saved: {OUT_VOCAB}  ({len(vocab)} terms)")

    df_features.to_parquet(OUT_FEATURES, index=False, compression="snappy")
    print(f"  saved: {OUT_FEATURES}  {df_features.shape}")

def build() -> None:
    df, corpus    = load_processed()
    matrix, vocab, train_cutoff = build_ml_tfidf(df, corpus)
    df_features   = build_merge_features(df)
    save_all(matrix, vocab, df_features)
    print("\n" + "=" * 65)
    print("NLP PIPELINE COMPLETE")
    print("=" * 65)
    print(f"TF-IDF:{matrix.shape}  (fit on train < {train_cutoff.date()})")
    print(f"Features for merge: {df_features.shape}")
    print(f"D+1 shift applied: alarm_date = ISW date + 1 day")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ISW NLP Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--build", action="store_true", help="Run the full pipeline")
    args = parser.parse_args()
    if args.build:
        build()
    else:
        parser.print_help()