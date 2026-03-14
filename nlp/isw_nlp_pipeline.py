"""
ISW NLP Pipeline
Reads ALREADY PROCESSED data from isw_cleaner.py
Adds ML-specific TF-IDF (500 features, fit on TRAIN only, no leakage).
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
from sklearn.feature_extraction.text import TfidfVectorizer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"

INPUT_CSV   = PROCESSED / "isw_clean.csv"
INPUT_TEXTS = PROCESSED / "isw_texts.json"

OUT_TFIDF    = PROCESSED / "tfidf_matrix_model.npz"
OUT_VOCAB    = PROCESSED / "tfidf_vocab_model.json"
OUT_FEATURES = PROCESSED / "isw_features_for_merge.csv"

# must match the train/test split used in train_models.py
TRAIN_CUTOFF    = pd.Timestamp("2025-01-01")
TFIDF_FEATURES  = 500   # ML model — 500 reduces noise vs EDA processor's 5000
MASS_ATTACK_THR = 15

def _normalize_weapons(text: str) -> str:
    return re.sub(r'(?<=[a-zA-Z0-9])-(?=[0-9])', '', text)

def load_processed() -> tuple[pd.DataFrame, list[str]]:
    print("=" * 65)
    print("  STEP 1/3: Load processed ISW data")
    print("=" * 65)

    missing = [p for p in [INPUT_CSV, INPUT_TEXTS] if not p.exists()]
    if missing:
        print("ERROR: missing input files — run isw_processor.py --process first")
        for p in missing:
            print(f"    {p}")
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV)
    df["date"] = pd.to_datetime(df["date"])

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

    corpus_norm = [_normalize_weapons(c) for c in corpus]

    train_mask  = df["date"] < TRAIN_CUTOFF
    train_texts = [c for c, m in zip(corpus_norm, train_mask) if m]
    n_train     = len(train_texts)
    n_total     = len(corpus_norm)

    print(f"  train: {n_train} docs (before {TRAIN_CUTOFF.date()})")
    print(f"  test:  {n_total - n_train} docs")
    print(f"  features: {TFIDF_FEATURES} (vs 5000 in EDA processor)")
    print(f"  NOTE: TRAIN_CUTOFF must match split date in train_models.py (Task 4)")

    vectorizer = TfidfVectorizer(
        max_features=TFIDF_FEATURES,
        stop_words="english",
        ngram_range=(1, 2),
        min_df=5,
        max_df=0.9,
        sublinear_tf=True,
        token_pattern=r"(?u)\b[a-zA-Z0-9]{3,}\b",
    )
    vectorizer.fit(train_texts)                  # fit on TRAIN only — no leakage
    matrix = vectorizer.transform(corpus_norm)   # transform on all (train + test)
    vocab  = vectorizer.get_feature_names_out().tolist()

    print(f"  matrix: {matrix.shape}")
    expected = ["attack", "strike", "missile", "drone", "forces",
                "russian", "ukrainian", "f16", "su34", "s300"]
    found   = [w for w in expected if w in vocab]
    missing = [w for w in expected if w not in vocab]
    print(f"  war terms in vocab: {found}")
    if missing:
        print(f"  NOT in vocab (too rare / filtered by min_df): {missing}")
    return matrix, vocab

def build_merge_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 65)
    print("  STEP 3/3: Build merge features with D+1 shift")
    print("=" * 65)

    cols_for_model = [
        "date",
        "isw_report_length",
        "isw_sources_count",
        "attack_mentions",
        "ground_mentions",
        "casualty_mentions",
        "total_intensity",
        "intensity_per_1000",
        "unique_domains",
    ]
    available = [c for c in cols_for_model if c in df.columns]
    dropped   = [c for c in cols_for_model if c not in df.columns]
    if dropped:
        print(f"  NOTE: columns not in isw_clean.csv (skipped): {dropped}")

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

    df_features.to_csv(OUT_FEATURES, index=False)
    print(f"  saved: {OUT_FEATURES}  {df_features.shape}")

def build() -> None:
    df, corpus    = load_processed()
    matrix, vocab = build_ml_tfidf(df, corpus)
    df_features   = build_merge_features(df)
    save_all(matrix, vocab, df_features)
    print("\n" + "=" * 65)
    print("NLP PIPELINE COMPLETE")
    print("=" * 65)
    print(f"TF-IDF:{matrix.shape}  (fit on train < {TRAIN_CUTOFF.date()})")
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