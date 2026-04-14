import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "gur"
PROCESSED = PROJECT_ROOT / "data" / "processed"
OUTPUT_CSV = PROCESSED / "gur_features_clean.csv"

def load_raw_gur() -> pd.DataFrame:
    records = []
    for f in sorted(RAW_DIR.glob("*.json")):
        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            date_str = data.get("date", f.stem)
            articles = data.get("articles", [])
            day_record = {
                "date": date_str,
                "n_articles": len(articles),
                "kw_success_total": sum(a.get("kw_success", 0) for a in articles),
                "kw_retaliation_total": sum(a.get("kw_retaliation", 0) for a in articles),
                "kw_logistics_total": sum(a.get("kw_logistics", 0) for a in articles),
                "kw_energy_total": sum(a.get("kw_energy", 0) for a in articles),
                "kw_intercepts_total": sum(a.get("kw_intercepts", 0) for a in articles)}
            records.append(day_record)
        except (json.JSONDecodeError, KeyError):
            continue
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop = True)
    return df

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["date"] = df["date"]
    out["gur_massive_success_d1"] = (df["kw_success_total"] > 0).astype(np.int8)
    retaliation_3d = df["kw_retaliation_total"].rolling(3, min_periods=1).sum()
    out["gur_retaliation_72h"] = (retaliation_3d > 0).astype(np.int8)
    logistics_14d = df["kw_logistics_total"].rolling(14, min_periods=1).sum()
    max_val = logistics_14d.max()
    if max_val > 0:
        out["gur_logistics_14d"] = (logistics_14d / max_val).round(3).astype(np.float32)
    else:
        out["gur_logistics_14d"] = np.float32(0.0)
    out["gur_energy_threat_d1"] = (df["kw_energy_total"] > 0).astype(np.int8)
    intercepts_7d = df["kw_intercepts_total"].rolling(7, min_periods=1).sum()
    out["gur_intercepts_7d"] = (intercepts_7d > 0).astype(np.int8)
    out["alarm_date"] = out["date"] + pd.Timedelta(days=1)
    result = out.drop(columns=["date"])
    return result

def build() -> None:
    PROCESSED.mkdir(parents=True, exist_ok=True)
    print("GUR NLP PROCESSOR")
    if not RAW_DIR.exists() or not any(RAW_DIR.glob("*.json")):
        print(f"No data in {RAW_DIR}")
        return

    df_raw = load_raw_gur()
    if df_raw.empty:
        print("Data is not uploaded")
        return
    print(f"  GUR raw: {len(df_raw):,} days ({df_raw.date.min().date()} → {df_raw.date.max().date()})")
    df_features = build_features(df_raw)
    df_features.to_csv(OUTPUT_CSV, index = False)
    print(f"\n Saved: {OUTPUT_CSV}")
    print(f"  Shape: {df_features.shape}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action = "store_true")
    args = parser.parse_args()
    if args.build:
        build()
    else:
        parser.print_help()