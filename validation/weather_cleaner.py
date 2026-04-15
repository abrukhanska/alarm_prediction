"""
--process: full reprocess from JSON files in data/raw/weather/historical/
--incremental: append new rows from CSVs in data/raw/weather/new/ to weather_clean.csv
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HISTORICAL_DIR = PROJECT_ROOT / "data" / "raw" / "weather" / "historical"
NEW_DIR = PROJECT_ROOT / "data" / "raw" / "weather" / "new"
OUTPUT_CSV = PROJECT_ROOT / "data" / "processed" / "weather_clean.csv"
REPORT_TXT = PROJECT_ROOT / "data" / "processed" / "weather_processing_report.txt"

EXPECTED_HOURS = 24
MAX_FILL_HOURS = 6
WAR_START = pd.Timestamp("2022-02-24")

RANGES = {"hour_temp": (-50.0,  50.0),
          "hour_humidity": (  0.0, 100.0),
          "hour_windspeed": (  0.0, 200.0),
          "hour_pressure": (870.0,1084.0),
          "hour_cloudcover": (  0.0, 100.0),
          "hour_visibility": (  0.0,  50.0),
          "hour_winddir": (  0.0, 360.0),
          "hour_precip": (  0.0, 200.0),
          "hour_windgust": (  0.0, 300.0),
          "hour_feelslike": (-60.0,  60.0) }

UKRAINE_LAT = (44.0, 53.0)
UKRAINE_LON = (22.0, 41.0)

DAILY_SKIP = {"latitude", "longitude", "city"}
EXPECTED_REGIONS = ["Vinnytsia", "Lutsk",   "Dnipro",    "Donetsk",  "Zhytomyr",
                    "Uzhgorod",  "Zaporozhye", "Ivano-Frankivsk", "Kyiv",
                    "Kropyvnytskyi", "Lviv", "Mykolaiv", "Odesa",
                    "Poltava",   "Rivne",   "Sumy",      "Ternopil", "Kharkiv",
                    "Kherson",   "Khmelnytskyi", "Cherkasy", "Chernivtsi",
                    "Chernihiv", "Luhansk" ]

NUMERIC_COLS = [ "day_tempmax", "day_tempmin", "day_temp", "day_humidity",
                 "day_precip",  "day_windspeed", "day_cloudcover", "day_visibility",
                 "hour_dew",    "hour_precipprob", "hour_snow", "hour_snowdepth"]

CLEAN_DTYPES: dict = {"city_address": "category",
                      "season":       "int8",
                      "is_night":     "int8",
                      "is_rain":      "int8",
                      "is_snow":      "int8",
                      "hour":         "int8",
                      "day_of_week":  "int8",
                      "month":        "int8", }

CITY_NORMALIZE: dict[str, str] = {"Zaporizhzhia": "Zaporozhye", "Uzhhorod":     "Uzhgorod", "Uzhgorod":     "Uzhgorod",}

def safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).strip()
        return None if s == "" else float(s)
    except (ValueError, TypeError):
        return None

def _normalize_city(addr: str) -> str:
    city = str(addr).split(",")[0].strip()
    return CITY_NORMALIZE.get(city, city)

def _core_clean(df: pd.DataFrame, errors: list, cutoff: pd.Timestamp) -> pd.DataFrame:
    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["datetime_hour"])
    if len(df) < before:
        errors.append(f"DATETIME: dropped {before - len(df)} unparseable rows")
    pre_war = (df["datetime_hour"] < WAR_START).sum()
    if pre_war:
        errors.append(f"PRE-WAR: {pre_war} rows before {WAR_START.date()} - dropped")
        df = df[df["datetime_hour"] >= WAR_START]
    future = (df["datetime_hour"] > cutoff).sum()
    if future:
        errors.append(f"FUTURE: {future} rows after {cutoff.date()} - dropped")
        df = df[df["datetime_hour"] <= cutoff]

    df["city_address"] = df["city_address"].apply(_normalize_city)
    for col in ["city_latitude", "city_longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "city_latitude" in df.columns and "city_longitude" in df.columns:
        has_coords = df["city_latitude"].notna() & df["city_longitude"].notna()
        outside = has_coords & ~(
            df["city_latitude"].between(*UKRAINE_LAT)
            & df["city_longitude"].between(*UKRAINE_LON)
        )
        if outside.sum():
            bad = sorted(df.loc[outside, "city_address"].unique())
            errors.append(f"GEOGRAPHY: {outside.sum()} rows outside Ukraine - {bad}")
            df = df[~outside]

    if "hour_visibility" in df.columns:
        empty = (df["hour_visibility"].astype(str).str.strip() == "").sum()
        if empty:
            errors.append(f"VISIBILITY: {empty} empty strings - NaN")
        df["hour_visibility"] = df["hour_visibility"].replace("", np.nan)

    for col, (lo, hi) in RANGES.items():
        if col not in df.columns:
            continue
        pre_nan = df[col].isna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        invalid = df[col].isna().sum() - pre_nan
        if invalid > 0:
            errors.append(f"FORMAT: {col} - {invalid} non-numeric → NaN")
        outliers = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
        if outliers.sum():
            errors.append(f"RANGE: {col} - {outliers.sum()} anomalies outside [{lo},{hi}] - NaN")
            df.loc[outliers, col] = np.nan

    for col in NUMERIC_COLS:
        if col in df.columns and df[col].dtype == "object":
            pre = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            new_nan = df[col].isna().sum() - pre
            if new_nan > 0:
                errors.append(f"FORMAT: {col} - {new_nan} non-numeric - NaN")

    df = df.sort_values(["city_address", "datetime_hour"])
    before = len(df)
    df = df.drop_duplicates(subset=["city_address", "datetime_hour"]).reset_index(drop=True)
    dupes = before - len(df)
    if dupes:
        errors.append(f"DUPLICATES: removed {dupes}")
    fill_cols = [c for c in RANGES if c in df.columns]
    nans_before = df[fill_cols].isna().sum().sum()
    for col in fill_cols:
        df[col] = df.groupby("city_address", group_keys=False)[col].apply(
            lambda x: x.ffill(limit=MAX_FILL_HOURS).bfill(limit=MAX_FILL_HOURS))
    nans_after = df[fill_cols].isna().sum().sum()
    filled = nans_before - nans_after
    if filled:
        errors.append(f"FILL: filled {filled} NaN (limit={MAX_FILL_HOURS}h)")
    if nans_after:
        errors.append(f"FILL: {nans_after} NaN remain - gaps > {MAX_FILL_HOURS}h")

    found = set(df["city_address"].unique())
    missing_r = set(EXPECTED_REGIONS) - found
    unexpected_r = found - set(EXPECTED_REGIONS)
    if missing_r:
        errors.append(f"REGIONS MISSING: {sorted(missing_r)}")
    if unexpected_r:
        n = df[df["city_address"].isin(unexpected_r)].shape[0]
        errors.append(f"REGIONS UNEXPECTED ({n} rows): {sorted(unexpected_r)}")
    df = df[df["city_address"].isin(EXPECTED_REGIONS)]

    df["hour"] = df["datetime_hour"].dt.hour
    df["day_of_week"] = df["datetime_hour"].dt.dayofweek
    df["month"] = df["datetime_hour"].dt.month
    df["is_night"] = (df["hour"] < 6).astype(int)
    df["season"] = df["month"].map({12: 1, 1: 1, 2: 1, 3: 2, 4: 2, 5: 2,
                                    6: 3, 7: 3, 8: 3, 9: 4, 10: 4, 11: 4})

    if "hour_conditions" in df.columns:
        cond = df["hour_conditions"].astype(str).str.lower()
        df["is_rain"] = cond.str.contains("rain", na=False).astype(int)
        df["is_snow"] = cond.str.contains("snow", na=False).astype(int)
    else:
        df["is_rain"] = 0
        df["is_snow"] = 0

    if "hour_temp" in df.columns and "hour_feelslike" in df.columns:
        df["temp_diff"] = df["hour_temp"] - df["hour_feelslike"]
    else:
        df["temp_diff"] = np.nan

    if "hour_pressure" in df.columns and "day_pressure" in df.columns:
        df["day_pressure"]  = pd.to_numeric(df["day_pressure"],  errors="coerce")
        df["hour_pressure"] = pd.to_numeric(df["hour_pressure"], errors="coerce")
        df["pressure_trend"] = df["hour_pressure"] - df["day_pressure"]
    else:
        df["pressure_trend"] = np.nan

    drop_cols = [
        c for c in ["hour_datetime", "day_conditions", "hour_conditions",
                    "day_icon", "hour_icon"]
        if c in df.columns
    ]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df

def _load_json_to_df(errors: list) -> pd.DataFrame:
    all_files = sorted(HISTORICAL_DIR.glob("**/*.json"))
    if not all_files:
        print(f"ERROR: No JSON files found in {HISTORICAL_DIR}")
        return pd.DataFrame()

    print(f"Found {len(all_files)} JSON files")
    records = []
    skipped = 0

    for fp in all_files:
        city = fp.parent.name.replace("_", " ").strip()
        try:
            with open(fp, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            errors.append(f"FILE ERROR: {fp.name} - {e}")
            skipped += 1
            continue

        if "daily" in data and isinstance(data["daily"], dict):
            daily_data = data["daily"]
            day_date = daily_data.get("day_datetime")
            lat = safe_float(daily_data.get("latitude"))
            lon = safe_float(daily_data.get("longitude"))
            hours = data.get("hours", [])

        elif "days" in data and isinstance(data["days"], list) and data["days"]:
            day_obj = data["days"][0]
            day_date = day_obj.get("datetime")
            lat = safe_float(data.get("latitude"))
            lon = safe_float(data.get("longitude"))
            hours = day_obj.get("hours", [])
            daily_data = {}
            for k, v in day_obj.items():
                if k in ("datetime", "hours"):
                    continue
                col = k if k.startswith("day_") else f"day_{k}"
                daily_data[col] = v

        else:
            errors.append(f"FORMAT: {fp.name} - unknown JSON structure")
            skipped += 1
            continue

        if not isinstance(hours, list):
            errors.append(f"FORMAT: {fp.name} - 'hours' is not a list")
            skipped += 1
            continue

        if len(hours) != EXPECTED_HOURS:
            errors.append(f"CHRONOLOGY: {fp.name} ({city}) - {len(hours)}h")

        for hour in hours:
            if not isinstance(hour, dict):
                continue
            row = {"city_address": city, "city_latitude": lat, "city_longitude": lon}
            for key, value in daily_data.items():
                if key in DAILY_SKIP:
                    continue
                col_name = key if key.startswith("day_") else f"day_{key}"
                row[col_name] = value
            for key, value in hour.items():
                if key not in row:
                    row[key] = value
            h_time = hour.get("hour_datetime")
            row["datetime_hour"] = (f"{day_date} {h_time}" if day_date and h_time else h_time)
            records.append(row)

    if skipped:
        errors.append(f"FILES SKIPPED: {skipped} / {len(all_files)}")

    df = pd.DataFrame(records)
    if "city_address" not in df.columns and "city" in df.columns:
        df = df.rename(columns={"city": "city_address"})
    return df

def _load_new_csv(errors: list, cutoff_dt: pd.Timestamp | None = None) -> pd.DataFrame:
    if not NEW_DIR.exists():
        print(f"No new weather directory: {NEW_DIR}")
        return pd.DataFrame()

    csv_files = sorted(NEW_DIR.glob("*_weather_raw.csv"))
    if not csv_files:
        print(f"No *_weather_raw.csv files in {NEW_DIR}")
        return pd.DataFrame()

    if cutoff_dt is not None:
        cutoff_date = cutoff_dt.date()
        before_filter = len(csv_files)
        csv_files = [
            fp for fp in csv_files
            if _date_from_filename(fp) is not None
            and _date_from_filename(fp) >= cutoff_date
        ]
        skipped = before_filter - len(csv_files)
        if skipped:
            print(f"Skipped {skipped} already-processed file(s) (date < {cutoff_date})")

    if not csv_files:
        print("No new CSV files to process.")
        return pd.DataFrame()

    print(f"Found {len(csv_files)} new CSV file(s)")
    frames = []
    for fp in csv_files:
        try:
            df = pd.read_csv(fp, low_memory=False)
            if "city" in df.columns and "city_address" not in df.columns:
                df = df.rename(columns={"city": "city_address"})
            frames.append(df)
            print(f"{fp.name}: {len(df):,} rows")
        except Exception as e:
            errors.append(f"CSV ERROR: {fp.name} - {e}")

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    print(f"New CSV total rows: {len(df):,}")
    return df

def _date_from_filename(fp: Path) -> "date | None":
    from datetime import date as date_type
    try:
        return date_type.fromisoformat(fp.stem.split("_")[0])
    except (ValueError, IndexError):
        return None

def _print_summary(df: pd.DataFrame, n_raw: int, errors: list, mode: str) -> None:
    region_counts = df.groupby("city_address").size()
    print()
    print("-" * 60)
    print(f"WEATHER PROCESSING COMPLETE  [{mode}]")
    print("-" * 60)
    print(f"Input: {n_raw:,} rows")
    print(f"Output: {df.shape[0]:,} rows × {df.shape[1]} cols")
    print(f"Regions: {df['city_address'].nunique()} / {len(EXPECTED_REGIONS)}")
    print(f"Date range: {df['datetime_hour'].min()} → {df['datetime_hour'].max()}")
    print(f"Smallest: {region_counts.idxmin()} ({region_counts.min():,})")
    print(f"Largest: {region_counts.idxmax()} ({region_counts.max():,})")
    print(f"Issues: {len(errors)}")
    if errors:
        print()
        for i, e in enumerate(errors, 1):
            print(f"{i:>3}. {e}")
    object_cols = [
        c for c in df.columns
        if df[c].dtype == "object"
        and c not in ("city_address", "day_datetime", "hour_preciptype",
                      "day_preciptype", "day_stations", "hour_stations",
                      "day_source", "hour_source")
    ]
    if object_cols:
        print(f"\n  WARNING: {len(object_cols)} cols still object dtype: {object_cols}")
    else:
        print("\n  All numeric columns have correct dtype.")
    print("-" * 60)

def _save_report(df: pd.DataFrame, n_raw: int, errors: list, mode: str) -> None:
    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write(f"Weather processing report  [{mode}]\n")
        f.write("-" * 50 + "\n\n")
        f.write(f"Input rows: {n_raw:,}\n")
        f.write(f"Clean rows: {len(df):,}\n")
        f.write(f"Columns: {len(df.columns)}\n")
        f.write(f"Retention: {len(df)/n_raw*100:.1f}%\n" if n_raw else "Retention:   n/a\n")
        f.write(f"Regions: {df['city_address'].nunique()}/{len(EXPECTED_REGIONS)}\n")
        f.write(f"Date range: {df['datetime_hour'].min()} → {df['datetime_hour'].max()}\n")
        f.write(f"Issues: {len(errors)}\n\n")
        f.write("Columns:\n")
        for i, c in enumerate(df.columns, 1):
            nans = df[c].isna().sum()
            f.write(f"{i:>2}. {c:35s} {str(df[c].dtype):12s} NaN: {nans:>8,}\n")
        if errors:
            f.write("\nIssues:\n")
            for i, e in enumerate(errors, 1):
                f.write(f"  {i:>3}. {e}\n")
    print(f"Report: {REPORT_TXT}")

def process() -> None:
    errors: list[str] = []
    cutoff = pd.Timestamp.now().normalize()
    print(f"\n{'-'*65}")
    print("WEATHER PROCESSOR  [FULL MODE]")
    print(f"{'-'*65}")
    print(f"Source: {HISTORICAL_DIR}")
    print(f"Cutoff: {cutoff.date()}")

    df = _load_json_to_df(errors)
    if df.empty:
        print("ERROR: No data loaded from JSON files")
        return
    n_raw = len(df)
    print(f"Extracted: {n_raw:,} raw records")

    df = _core_clean(df, errors, cutoff)
    if df.empty:
        print("ERROR: DataFrame empty after cleaning")
        return

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n  Saved: {OUTPUT_CSV}  ({df.shape[0]:,} × {df.shape[1]})")

    _save_report(df, n_raw, errors, "FULL")
    _print_summary(df, n_raw, errors, "FULL")

def process_incremental() -> None:
    errors: list[str] = []
    cutoff = pd.Timestamp.now().normalize()

    print(f"\n{'-'*65}")
    print("WEATHER PROCESSOR  [INCREMENTAL MODE]")
    print(f"{'-'*65}")

    if not OUTPUT_CSV.exists():
        print("No existing weather_clean.csv — run --process first.")
        return

    _dtypes = {**CLEAN_DTYPES,
               **{c: "float32" for c in list(RANGES.keys()) + NUMERIC_COLS
                  if c not in CLEAN_DTYPES}}
    existing = pd.read_csv(OUTPUT_CSV, parse_dates=["datetime_hour"], dtype=_dtypes)
    print(f"Existing rows: {len(existing):,}")
    print(f"Existing range: {existing['datetime_hour'].min().date()} - "
          f"{existing['datetime_hour'].max().date()}")

    max_existing_dt = existing["datetime_hour"].max()
    print(f"Loading CSVs after: {max_existing_dt.date()}")

    df_raw = _load_new_csv(errors, cutoff_dt=max_existing_dt)
    if df_raw.empty:
        print("No new data to process.")
        return

    n_raw = len(df_raw)
    df_new = _core_clean(df_raw, errors, cutoff)

    if df_new.empty:
        print("No valid new rows after cleaning.")
        return

    print(f"New clean rows: {len(df_new):,}")

    combined = (pd.concat([existing, df_new], ignore_index=True)
                  .drop_duplicates(subset=["city_address", "datetime_hour"], keep="last")
                  .sort_values(["city_address", "datetime_hour"])
                  .reset_index(drop=True))
    combined.to_csv(OUTPUT_CSV, index=False)
    truly_new = len(combined) - len(existing)

    if errors:
        print(f"\n  Issues ({len(errors)}):")
        for iss in errors:
            print(f"    . {iss}")

    print(f"\n{'-'*65}")
    print("INCREMENTAL COMPLETE")
    print(f"New rows added: {truly_new:,}")
    print(f"Total rows: {len(combined):,} → {OUTPUT_CSV.name}")
    print(f"New range max: {combined['datetime_hour'].max().date()}")
    print(f"{'-'*65}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process weather data to weather_clean.csv",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=__doc__,)
    parser.add_argument("--process", action="store_true",
                        help="Full reprocess from JSON files (run once to build base)",)
    parser.add_argument("--incremental", action="store_true",
                        help="Append only new CSV rows to existing weather_clean.csv",)
    args = parser.parse_args()
    if args.process:
        process()
    elif args.incremental:
        process_incremental()
    else:
        parser.print_help()