import argparse
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import scipy.sparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = PROJECT_ROOT / "data" / "processed"

WEATHER_CSV = PROCESSED / "weather_clean.csv"
ALARMS_CSV = PROCESSED / "alarms_clean.csv"
ISW_CSV = PROCESSED / "isw_features_for_merge.csv"
TFIDF_NPZ = PROCESSED / "tfidf_matrix_model.npz"
TFIDF_VOCAB = PROCESSED / "tfidf_vocab_model.json"

OUTPUT_CSV = PROCESSED / "merged_dataset.csv"
REPORT_TXT = PROCESSED / "merge_report.txt"

# must match isw_nlp_pipeline.py and train_models.py
TRAIN_CUTOFF = pd.Timestamp("2025-01-01")
KYIV_TZ = "Europe/Kyiv"

def _to_kyiv_naive(series: pd.Series) -> pd.Series:
    if series.dt.tz is None:
        return series
    return series.dt.tz_convert(KYIV_TZ).dt.tz_localize(None)

WEATHER_TO_ALARM = {
    "Vinnytsia": "Vinnytsia Oblast",
    "Lutsk": "Volyn Oblast",
    "Dnipro": "Dnipropetrovsk Oblast",
    "Donetsk": "Donetsk Oblast",
    "Zhytomyr": "Zhytomyr Oblast",
    "Uzhgorod": "Zakarpattia Oblast",
    "Zaporozhye": "Zaporizhzhia Oblast",
    "Ivano-Frankivsk": "Ivano-Frankivsk Oblast",
    "Kyiv": "City of Kyiv",
    "Kropyvnytskyi": "Kirovohrad Oblast",
    "Lviv": "Lviv Oblast",
    "Mykolaiv": "Mykolaiv Oblast",
    "Odesa": "Odesa Oblast",
    "Poltava": "Poltava Oblast",
    "Rivne": "Rivne Oblast",
    "Sumy": "Sumy Oblast",
    "Ternopil": "Ternopil Oblast",
    "Kharkiv": "Kharkiv Oblast",
    "Kherson": "Kherson Oblast",
    "Khmelnytskyi": "Khmelnytskyi Oblast",
    "Cherkasy": "Cherkasy Oblast",
    "Chernivtsi": "Chernivtsi Oblast",
    "Chernihiv": "Chernihiv Oblast",
}

WEATHER_FEATURE_COLS = [
    "datetime_hour", "city_address",
    "hour_temp", "hour_feelslike", "hour_humidity", "hour_dew",
    "hour_precip", "hour_precipprob", "hour_snow", "hour_snowdepth",
    "hour_windgust", "hour_windspeed", "hour_winddir", "hour_pressure",
    "hour_visibility", "hour_cloudcover",
    "is_night", "is_rain", "is_snow", "temp_diff", "pressure_trend", "season"
]


def load_inputs() -> tuple:
    print("=" * 65)
    print("STEP 1/5: Load inputs")
    print("=" * 65)
    required = [WEATHER_CSV, ALARMS_CSV, ISW_CSV, TFIDF_NPZ, TFIDF_VOCAB]
    missing = [p for p in required if not p.exists()]
    if missing:
        print("ERROR: missing files:")
        for p in missing:
            print(f"{p}")
        sys.exit(1)

    df_w_raw = pd.read_csv(WEATHER_CSV)
    keep = [c for c in WEATHER_FEATURE_COLS if c in df_w_raw.columns]
    dropped = [c for c in df_w_raw.columns if c not in keep]
    if dropped:
        print(f"  weather: dropping {len(dropped)} non-feature columns")

    df_w = df_w_raw[keep].copy()
    df_w["datetime_hour"] = _to_kyiv_naive(pd.to_datetime(df_w["datetime_hour"], utc=False))
    print(f"  weather:  {df_w.shape}  |  {df_w.city_address.nunique()} cities")

    with open(ALARMS_CSV, 'r', encoding='utf-8') as f:
        first_line = f.readline()
    sep = ';' if ';' in first_line else ','
    df_a = pd.read_csv(ALARMS_CSV, sep=sep)

    df_a["start_dt"] = _to_kyiv_naive(pd.to_datetime(df_a["start_dt"], utc=False))
    df_a["end_dt"] = _to_kyiv_naive(pd.to_datetime(df_a["end_dt"], utc=False))
    print(f"  alarms:   {df_a.shape}  |  {df_a.region.nunique()} regions")

    df_i = pd.read_csv(ISW_CSV)
    if "date" in df_i.columns:
        df_i = df_i.drop(columns=["date"])
    df_i["alarm_date"] = pd.to_datetime(df_i["alarm_date"]).dt.normalize()
    print(f"  ISW:      {df_i.shape}  |  {df_i.alarm_date.min().date()} -> {df_i.alarm_date.max().date()}")

    tfidf = scipy.sparse.load_npz(TFIDF_NPZ)
    with open(TFIDF_VOCAB, "r", encoding="utf-8") as f:
        vocab = json.load(f)
    print(f"  TF-IDF:   {tfidf.shape}  |  {len(vocab)} terms")

    return df_w, df_a, df_i, tfidf, vocab


def build_alarm_matrix(
        df_a: pd.DataFrame,
        max_hour: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n" + "=" * 65)
    print("  STEP 2/5: Expand alarms to hourly")
    print("=" * 65)

    open_alarms = df_a["end_dt"].isna().sum()
    if open_alarms:
        print(f"  open alarms (end_dt=NaN): {open_alarms} → extending to {max_hour}")

    max_h = max_hour.floor("h")

    df_a = df_a.copy()
    df_a["start_h"] = df_a["start_dt"].dt.floor("h")
    end_dt_adj = np.maximum(df_a["end_dt"].fillna(max_h), df_a["start_dt"] + pd.Timedelta(seconds=1))
    df_a["end_h"] = (end_dt_adj - pd.Timedelta(seconds=1)).dt.floor("h")
    df_a["end_h"] = df_a[["start_h", "end_h"]].max(axis=1)

    print(f"  expanding {len(df_a):,} alarm events via date_range...")
    chunks = []
    for region, start_h, end_h in zip(df_a["region"], df_a["start_h"], df_a["end_h"]):
        hours = pd.date_range(start=start_h, end=end_h, freq="h")
        chunks.append(pd.DataFrame({"region": region, "datetime_hour": hours}))

    df_expanded = pd.concat(chunks, ignore_index=True)

    df_n_regions = (
        df_expanded
        .drop_duplicates(subset=["region", "datetime_hour"])
        .groupby("datetime_hour", sort=True)
        .agg(n_regions_alarm=("region", "nunique"))
        .reset_index()
    )

    df_alarm = (
        df_expanded
        .drop_duplicates(subset=["region", "datetime_hour"])
        .assign(alarm=1)
    )

    mappable = set(WEATHER_TO_ALARM.values())
    unmapped = df_alarm[~df_alarm["region"].isin(mappable)]["region"].unique()
    if len(unmapped):
        print(f"  regions without weather station (counted in n_regions_alarm, no model rows):")
        for r in sorted(unmapped):
            cnt = df_alarm[df_alarm["region"] == r].shape[0]
            print(f"    {r}: {cnt:,} alarm-hours")
    df_alarm = df_alarm[df_alarm["region"].isin(mappable)]

    print(f"  expanded: {df_alarm.shape[0]:,} (region, hour) pairs")
    print(f"  n_regions_alarm: max={df_n_regions['n_regions_alarm'].max()}, "
          f"mass_attack_hours(>15)={(df_n_regions['n_regions_alarm'] > 15).sum():,}")

    return df_alarm, df_n_regions

def build_isw_daily(
        df_i: pd.DataFrame,
        tfidf: scipy.sparse.csr_matrix,
        vocab: list[str],
) -> pd.DataFrame:
    print("\n" + "=" * 65)
    print("  STEP 3/5: Build ISW + TF-IDF daily features")
    print("=" * 65)

    assert tfidf.shape[0] == len(df_i), (
        f"TF-IDF rows ({tfidf.shape[0]}) != ISW rows ({len(df_i)}). "
        f"Re-run isw_nlp_pipeline.py --build"
    )

    print("  Converting TF-IDF matrix to DataFrame (Optimized)...")
    dense_matrix = np.round(tfidf.toarray(), 4).astype(np.float32)
    df_tfidf = pd.DataFrame(
        dense_matrix,
        index=df_i.index,
        columns=[f"tfidf_{v}" for v in vocab]
    )

    df_isw_full = pd.concat([df_i, df_tfidf], axis=1)

    print(f"  ISW scalar cols: {len(df_i.columns) - 1}")
    print(f"  TF-IDF cols:     {len(vocab)}")
    return df_isw_full

def merge_all(
        df_w: pd.DataFrame,
        df_alarm: pd.DataFrame,
        df_n_regions: pd.DataFrame,
        df_isw_full: pd.DataFrame,
) -> pd.DataFrame:
    print("\n" + "=" * 65)
    print("  STEP 4/5: Merge")
    print("=" * 65)
    df_w = df_w.copy()

    df_w["_city_match"] = df_w["city_address"].str.split(',').str[0].str.strip()
    df_w["region"] = df_w["_city_match"].map(WEATHER_TO_ALARM)

    unmapped_cities = df_w[df_w["region"].isna()]["city_address"].unique()
    if len(unmapped_cities):
        print(f"  WARNING: cities not in WEATHER_TO_ALARM map: {unmapped_cities}")
        df_w = df_w.dropna(subset=["region"])

    df_w = df_w.drop(columns=["_city_match"])

    print(f"  weather backbone: {df_w.shape}")

    df = df_w.merge(
        df_alarm[["region", "datetime_hour", "alarm"]],
        on=["region", "datetime_hour"],
        how="left",
    )
    df["alarm"] = df["alarm"].fillna(0).astype(np.int8)
    print(f"  after alarm join:    {df.shape}  |  alarm rate={df['alarm'].mean() * 100:.2f}%")

    df = df.merge(df_n_regions, on="datetime_hour", how="left")
    df["n_regions_alarm"] = df["n_regions_alarm"].fillna(0).astype(np.int8)
    print(f"  after n_regions:     {df.shape}")

    df["_date"] = df["datetime_hour"].dt.normalize()
    df = df.merge(
        df_isw_full,
        left_on="_date",
        right_on="alarm_date",
        how="left",
    )

    drop_cols = ["_date", "alarm_date", "city_address", "conditions", "date"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")
    print(f"  after ISW join:      {df.shape}")

    isw_scalar_cols = [
        "isw_report_length", "word_count", "sentence_count", "paragraph_count",
        "avg_sentence_length", "isw_sources_count", "sources_resolved",
        "sources_dead", "sources_blocked", "unique_domains", "attack_mentions",
        "ground_mentions", "casualty_mentions", "total_intensity", "intensity_per_1000"
    ]
    tfidf_col_names = [c for c in df.columns if c.startswith("tfidf_")]

    nan_dates = df[df["isw_report_length"].isna()]["datetime_hour"].dt.date.unique()
    if len(nan_dates):
        print(f"  ISW NaN on {len(nan_dates)} dates → filling with 0")

    for col in isw_scalar_cols + tfidf_col_names:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df = df.sort_values(["region", "datetime_hour"]).reset_index(drop=True)
    return df

def validate_and_save(df: pd.DataFrame) -> None:
    print("\n" + "=" * 65)
    print("  STEP 5/5: Validate & Save")
    print("=" * 65)
    issues = []
    n_regions = df["region"].nunique()
    n_hours = df["datetime_hour"].nunique()
    expected = n_regions * n_hours
    actual = len(df)
    completeness = actual / expected * 100 if expected > 0 else 0
    print(f"  shape:         {df.shape}")
    print(f"  regions:       {n_regions}  |  unique hours: {n_hours:,}")
    print(f"  completeness:  {actual:,} / {expected:,} = {completeness:.1f}%")
    print(f"  date range:    {df.datetime_hour.min()} -> {df.datetime_hour.max()}")

    EXPECTED_ROWS = n_regions * n_hours
    if completeness < 95:
        issues.append(f"completeness below 95%: {completeness:.1f}%")

    for col in ["region", "datetime_hour", "alarm", "n_regions_alarm"]:
        n = df[col].isna().sum()
        if n:
            issues.append(f"NaN in critical column '{col}': {n}")

    alarm_rate = df["alarm"].mean() * 100
    if not (5 <= alarm_rate <= 50):
        issues.append(f"ALARM RATE: {alarm_rate:.2f}% (expected 5-50%)")
    print(f"  alarm rate:    {alarm_rate:.2f}%")

    max_n = df["n_regions_alarm"].max()
    mass_h = (df["n_regions_alarm"] > 15).sum()
    if max_n > 25:
        issues.append(f"n_regions_alarm max={max_n} > 25 total regions")
    print(f"  max n_regions: {max_n}  |  mass_attack_hours(>15): {mass_h:,}")

    train = df[df["datetime_hour"] < TRAIN_CUTOFF]
    test = df[df["datetime_hour"] >= TRAIN_CUTOFF]
    print(f"  train:         {len(train):,} rows (< {TRAIN_CUTOFF.date()})")
    print(f"  test:          {len(test):,} rows (>= {TRAIN_CUTOFF.date()})")
    if len(test) == 0:
        issues.append("TEST set is empty — check TRAIN_CUTOFF and data range")

    tfidf_cols = [c for c in df.columns if c.startswith("tfidf_")]
    print(f"  TF-IDF cols:   {len(tfidf_cols)}")
    if len(tfidf_cols) < 400:
        issues.append(f"Only {len(tfidf_cols)} TF-IDF columns (expected 500)")

    dupes = df.duplicated(subset=["region", "datetime_hour"]).sum()
    if dupes:
        issues.append(f"DUPLICATES: {dupes} rows with same (region, datetime_hour)")

    if issues:
        print(f"\n  Issues ({len(issues)}):")
        for iss in issues:
            print(f"    {iss}")
    else:
        print(f"\n  All validation checks passed.")

    PROCESSED.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"  saved: {OUTPUT_CSV}")

    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("MERGE REPORT\n" + "=" * 60 + "\n\n")
        f.write(f"Shape:         {df.shape}\n")
        f.write(f"Regions:       {n_regions}\n")
        f.write(f"Hours:         {n_hours:,}\n")
        f.write(f"Completeness:  {completeness:.1f}%\n")
        f.write(f"Alarm rate:    {alarm_rate:.2f}%\n")
        f.write(f"Train rows:    {len(train):,}\n")
        f.write(f"Test rows:     {len(test):,}\n")
        f.write(f"TF-IDF cols:   {len(tfidf_cols)}\n")
        f.write(f"Issues:        {len(issues)}\n\n")
        f.write("Columns:\n")
        for i, c in enumerate(df.columns, 1):
            nans = df[c].isna().sum()
            f.write(f"  {i:>3}. {c:40s} {str(df[c].dtype):10s} NaN:{nans:>8,}\n")
        if issues:
            f.write("\nIssues:\n")
            for iss in issues:
                f.write(f"  {iss}\n")
    print(f"  saved: {REPORT_TXT}")

def merge() -> None:
    df_w, df_a, df_i, tfidf, vocab = load_inputs()

    max_hour = df_w["datetime_hour"].max() if "datetime_hour" in df_w.columns else pd.Timestamp.now()

    df_alarm, df_n_regions = build_alarm_matrix(df_a, max_hour)
    df_isw_full = build_isw_daily(df_i, tfidf, vocab)
    df = merge_all(df_w, df_alarm, df_n_regions, df_isw_full)
    validate_and_save(df)

    print("\n" + "=" * 65)
    print("  MERGE COMPLETE")
    print("=" * 65)
    print(f"  Output: {OUTPUT_CSV}")
    print(f"  Shape:  {df.shape}")
    print(f"  Train/test split: {TRAIN_CUTOFF.date()}")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge weather + alarms + ISW into ML matrix (Task 2c)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--merge", action="store_true", help="run the merge")
    args = parser.parse_args()
    if args.merge:
        merge()
    else:
        parser.print_help()