import json
import argparse
from pathlib import Path
from typing import Any
import pandas as pd
import numpy as np

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
HISTORICAL_DIR = PROJECT_ROOT / "data" / "raw" / "weather" / "historical"
OUTPUT_CSV     = PROJECT_ROOT / "data" / "processed" / "weather_clean.csv"
REPORT_TXT     = PROJECT_ROOT / "data" / "processed" / "weather_cleaning_report.txt"

EXPECTED_HOURS = 24
MAX_FILL_HOURS = 6

RANGES = {"hour_temp":       (-50.0, 50.0),
          "hour_humidity":   (0.0, 100.0),
          "hour_windspeed":  (0.0, 200.0),
          "hour_pressure":   (870.0, 1084.0),
          "hour_cloudcover": (0.0, 100.0),
          "hour_visibility": (0.0, 50.0),
          "hour_winddir":    (0.0, 360.0),
          "hour_precip":     (0.0, 200.0),
          "hour_windgust":   (0.0, 300.0),
          "hour_feelslike":  (-60.0, 60.0)}

UKRAINE_LAT = (44.0, 53.0)
UKRAINE_LON = (22.0, 41.0)

WAR_START   = pd.Timestamp("2022-02-24")
DATA_CUTOFF = pd.Timestamp("2026-03-16")

DAILY_SKIP = {'latitude', 'longitude', 'city'}

EXPECTED_REGIONS = ['Vinnytsia', 'Lutsk', 'Dnipro', 'Donetsk', 'Zhytomyr',
    'Uzhgorod', 'Zaporozhye', 'Ivano-Frankivsk', 'Kyiv',
    'Kropyvnytskyi', 'Lviv', 'Mykolaiv', 'Odesa',
    'Poltava', 'Rivne', 'Sumy', 'Ternopil', 'Kharkiv', 'Kherson',
    'Khmelnytskyi', 'Cherkasy', 'Chernivtsi', 'Chernihiv']

def safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).strip()
        if s == "":
            return None
        return float(s)
    except (ValueError, TypeError):
        return None

def clean_and_save():
    records = []
    errors = []

    print(f"\nStarting weather cleaning...")
    print(f"Looking for JSON files in: {HISTORICAL_DIR}")
    print(f"Directory exists: {HISTORICAL_DIR.exists()}")

    all_files = sorted(list(HISTORICAL_DIR.glob("**/*.json")))
    if not all_files:
        all_files = sorted(list(HISTORICAL_DIR.glob("*.json")))
    if not all_files:
        all_files = sorted(list(HISTORICAL_DIR.glob("*/*.json")))

    if not all_files:
        print(f"ERROR: No JSON files found!")
        if HISTORICAL_DIR.exists():
            contents = list(HISTORICAL_DIR.iterdir())[:15]
            print(f"  Contents of {HISTORICAL_DIR}:")
            for item in contents:
                print(f"{'[DIR]' if item.is_dir() else '[FILE]'} {item.name}")
        return
    print(f"Found {len(all_files)} JSON files. Starting processing...")

    skipped_files = 0
    for fp in all_files:
        city = fp.parent.name.replace('_', ' ').strip()
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            errors.append(f"JSON ERROR: {fp.name} - {e}")
            skipped_files += 1
            continue
        except Exception as e:
            errors.append(f"FILE ERROR: {fp.name} - {e}")
            skipped_files += 1
            continue

        daily_data = data.get('daily', {})
        if not isinstance(daily_data, dict):
            daily_data = {}

        day_date = daily_data.get('day_datetime')
        lat = safe_float(daily_data.get('latitude'))
        lon = safe_float(daily_data.get('longitude'))

        hours = data.get('hours', [])
        if not isinstance(hours, list):
            errors.append(f"FORMAT: {fp.name} - 'hours' is not a list")
            skipped_files += 1
            continue

        if len(hours) != EXPECTED_HOURS:
            errors.append(f"CHRONOLOGY: {fp.name} ({city}) - {len(hours)}h")

        for hour in hours:
            if not isinstance(hour, dict):
                continue

            row = {}

            row['city_address']   = city
            row['city_latitude']  = lat
            row['city_longitude'] = lon

            for key, value in daily_data.items():
                if key in DAILY_SKIP:
                    continue
                col_name = key if key.startswith('day_') else f'day_{key}'
                row[col_name] = value

            for key, value in hour.items():
                if key not in row:
                    row[key] = value

            h_time = hour.get('hour_datetime')
            if day_date and h_time:
                row['datetime_hour'] = f"{day_date} {h_time}"
            else:
                row['datetime_hour'] = h_time
            records.append(row)

    if skipped_files:
        errors.append(f"FILES SKIPPED: {skipped_files} / {len(all_files)}")

    if not records:
        print("No records found!")
        return

    df = pd.DataFrame(records)
    initial_row_count = len(df)

    print(f"\nExtracted {initial_row_count:,} raw records")
    print(f"Columns from JSON: {len(df.columns)}")
    print(f"Column list: {df.columns.tolist()}")

    df['datetime_hour'] = pd.to_datetime(df['datetime_hour'], errors='coerce')
    before = len(df)
    df = df.dropna(subset=['datetime_hour'])
    if len(df) < before:
        errors.append(f"DATETIME: dropped {before - len(df)} rows with unparseable dates")

    pre_war = df[df['datetime_hour'] < WAR_START]
    if len(pre_war):
        regions_pre = sorted(pre_war['city_address'].unique())
        errors.append(f"EASTER EGG: {len(pre_war)} rows BEFORE {WAR_START.date()} from {regions_pre}")
        df = df[df['datetime_hour'] >= WAR_START]

    future = df[df['datetime_hour'] > DATA_CUTOFF]
    if len(future):
        errors.append(f"EASTER EGG: {len(future)} rows AFTER {DATA_CUTOFF.date()}")
        df = df[df['datetime_hour'] <= DATA_CUTOFF]

    for col in ['city_latitude', 'city_longitude']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    has_coords = df['city_latitude'].notna() & df['city_longitude'].notna()
    outside = has_coords & ~(df['city_latitude'].between(*UKRAINE_LAT) &df['city_longitude'].between(*UKRAINE_LON))
    if outside.sum():
        bad_regions = sorted(df.loc[outside, 'city_address'].unique())
        errors.append(f"GEOGRAPHY: {outside.sum()} rows outside Ukraine - {bad_regions}")
        df = df[~outside]

    no_coords = (~has_coords).sum()
    if no_coords:
        errors.append(f"GEOGRAPHY: {no_coords} rows have no lat/lon (kept)")

    if 'hour_visibility' in df.columns:
        empty_vis = (df['hour_visibility'].astype(str).str.strip() == '').sum()
        if empty_vis:
            errors.append(f"VISIBILITY: {empty_vis:,} empty strings to NaN")
        df['hour_visibility'] = df['hour_visibility'].replace('', np.nan)

    for col, (lo, hi) in RANGES.items():
        if col not in df.columns:
            continue
        pre_nan = df[col].isna().sum()
        df[col] = pd.to_numeric(df[col], errors='coerce')
        invalid = df[col].isna().sum() - pre_nan
        if invalid > 0:
            errors.append(f"FORMAT: {col} - {invalid} non-numeric to NaN")
        outliers = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
        bad = outliers.sum()
        if bad > 0:
            errors.append(f"RANGE: {col} - {bad} anomalies to NaN (outside [{lo},{hi}])")
            df.loc[outliers, col] = np.nan

    numeric_cols = ['day_tempmax', 'day_tempmin', 'day_temp', 'day_humidity',
        'day_precip', 'day_windspeed', 'day_cloudcover', 'day_visibility',
        'hour_dew', 'hour_precipprob', 'hour_snow', 'hour_snowdepth']
    converted_count = 0
    for col in numeric_cols:
        if col in df.columns and df[col].dtype == 'object':
            before_nan = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            after_nan = df[col].isna().sum()
            new_nan = after_nan - before_nan
            converted_count += 1
            if new_nan > 0:
                errors.append(f"FORMAT: {col} - {new_nan} non-numeric to NaN (bulk conversion)")
    if converted_count:
        print(f"Converted {converted_count} object columns to float64")

    df = df.sort_values(['city_address', 'datetime_hour'])
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['city_address', 'datetime_hour']).reset_index(drop=True)
    dupes = before_dedup - len(df)
    if dupes:
        errors.append(f"DUPLICATES: removed {dupes}")

    fill_cols = [c for c in RANGES if c in df.columns]
    nans_before = df[fill_cols].isna().sum().sum()

    for col in fill_cols:
        df[col] = df.groupby('city_address', group_keys=False)[col].apply(
            lambda x: x.ffill(limit=MAX_FILL_HOURS).bfill(limit=MAX_FILL_HOURS))

    nans_after = df[fill_cols].isna().sum().sum()
    filled = nans_before - nans_after
    if filled:
        errors.append(f"FILL: filled {filled} NaN, limit={MAX_FILL_HOURS}h")
    if nans_after:
        errors.append(f"FILL: {nans_after} NaN remain, gaps > {MAX_FILL_HOURS}h")

    gap_regions = []
    for region, group in df.groupby('city_address'):
        dt = group['datetime_hour'].sort_values()
        diffs = dt.diff().dropna()
        bad_gaps = diffs[diffs != pd.Timedelta(hours=1)]
        if len(bad_gaps):
            gap_regions.append(f"{region}: {len(bad_gaps)} gap(s)")
    if gap_regions:
        errors.append(f"TIME GAPS in {len(gap_regions)} region(s): {', '.join(gap_regions[:5])}")

    found = set(df['city_address'].unique())
    missing = set(EXPECTED_REGIONS) - found
    unexpected = found - set(EXPECTED_REGIONS)

    if missing:
        errors.append(f"REGIONS MISSING: {sorted(missing)}")
    if unexpected:
        n = df[df['city_address'].isin(unexpected)].shape[0]
        errors.append(f"REGIONS UNEXPECTED ({n} rows): {sorted(unexpected)}")

    print(f"\nAll regions found ({len(found)}):")
    for r in sorted(found):
        cnt = (df['city_address'] == r).sum()
        status = "ok" if r in EXPECTED_REGIONS else "NOT IN LIST"
        print(f"  '{r}' - {cnt:,} rows {status}")

    df = df[df['city_address'].isin(EXPECTED_REGIONS)]

    if df.empty:
        print("ERROR: DataFrame empty after region filter!")
        return

    df['hour'] = df['datetime_hour'].dt.hour
    df['day_of_week'] = df['datetime_hour'].dt.dayofweek
    df['month'] = df['datetime_hour'].dt.month
    df['is_night'] = ((df['hour'] >= 22) | (df['hour'] <= 5)).astype(int)

    if 'hour_conditions' in df.columns:
        conditions_lower = df['hour_conditions'].astype(str).str.lower()
        df['is_rain'] = conditions_lower.str.contains('rain', na=False).astype(int)
        df['is_snow'] = conditions_lower.str.contains('snow', na=False).astype(int)
    else:
        df['is_rain'] = 0
        df['is_snow'] = 0

    if 'hour_temp' in df.columns and 'hour_feelslike' in df.columns:
        df['temp_diff'] = df['hour_temp'] - df['hour_feelslike']
    else:
        df['temp_diff'] = np.nan

    if 'hour_pressure' in df.columns and 'day_pressure' in df.columns:
        df['day_pressure'] = pd.to_numeric(df['day_pressure'], errors='coerce')
        df['pressure_trend'] = df['hour_pressure'] - df['day_pressure']
    else:
        df['pressure_trend'] = np.nan

    df['season'] = df['month'].map({12: 1, 1: 1, 2: 1,
                                    3: 2, 4: 2, 5: 2,
                                    6: 3, 7: 3, 8: 3,
                                    9: 4, 10: 4, 11: 4})

    print(f"\nAfter feature engineering: {df.shape[0]:,} rows * {df.shape[1]} cols")
    drop_cols = []
    for col in ['hour_datetime', 'day_conditions', 'hour_conditions']:
        if col in df.columns:
            drop_cols.append(col)

    if drop_cols:
        df = df.drop(columns=drop_cols)
        print(f"Dropped redundant columns: {drop_cols}")

    print(f"Final shape: {df.shape[0]:,} rows * {df.shape[1]} cols")

    critical = ['city_address', 'datetime_hour', 'hour_temp', 'hour_humidity','hour_windspeed', 'hour_pressure']
    for col in critical:
        if col in df.columns:
            nans = df[col].isna().sum()
            if nans:
                errors.append(f"FINAL NaN: {col} - {nans} ({nans/len(df)*100:.2f}%)")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)

    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("Weather cleaning report\n")
        f.write("-" * 50 + "\n\n")
        f.write(f"Input rows: {initial_row_count:,}\n")
        f.write(f"Clean rows: {len(df):,}\n")
        f.write(f"Columns: {len(df.columns)} (target: 38)\n")
        f.write(f"Retention: {len(df)/initial_row_count*100:.1f}%\n")
        f.write(f"Regions: {df['city_address'].nunique()}/{len(EXPECTED_REGIONS)}\n")
        f.write(f"Date range: {df['datetime_hour'].min()} - {df['datetime_hour'].max()}\n")
        f.write(f"Issues: {len(errors)}\n\n")
        f.write("Column list:\n")
        for i, c in enumerate(df.columns, 1):
            f.write(f"{i:>2}. {c}\n")
        f.write(f"\nIssues detail:\n")
        for i, e in enumerate(errors, 1):
            f.write(f"{i:>3}. {e}\n")
    region_counts = df.groupby('city_address').size()

    print()
    print("-" * 60)
    print("  WEATHER CLEANING COMPLETED!")
    print("-" * 60)
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} cols")
    print(f"Target: 190,656 rows * 38 cols")
    print(f"Match cols: {'PASS' if df.shape[1] == 38 else f'GOT {df.shape[1]}'}")
    print(f"Retention: {len(df)/initial_row_count*100:.1f}%")
    print(f"Regions: {df['city_address'].nunique()} / {len(EXPECTED_REGIONS)}")
    print(f"Date range: {df['datetime_hour'].min()} -> {df['datetime_hour'].max()}")
    print(f"Smallest: {region_counts.idxmin()} ({region_counts.min():,})")
    print(f"Largest: {region_counts.idxmax()} ({region_counts.max():,})")
    print(f"Issues: {len(errors)}")

    if errors:
        print()
        print("Issues:")
        for i, e in enumerate(errors, 1):
            print(f"{i:>3}. {e}")

    print()
    print(f"Saved CSV: {OUTPUT_CSV}")
    print(f"Saved report: {REPORT_TXT}")
    print("-" * 60)

    print()
    print("Final columns ({}):" .format(df.shape[1]))
    object_cols = []
    for i, c in enumerate(df.columns, 1):
        dtype = df[c].dtype
        nans = df[c].isna().sum()
        flag = ""
        if dtype == 'object' and c not in ('city_address', 'day_datetime', 'hour_preciptype'):
            flag = "SHOULD BE NUMERIC!"
            object_cols.append(c)
        print(f"{i:>2}. {c:30s} {str(dtype):15s} NaN: {nans:>7,}{flag}")

    if object_cols:
        print(f"\n WARNING: {len(object_cols)} columns still object dtype: {object_cols}")
        print(f"These should be float64 for ML. Add them to numeric_cols list!")
    else:
        print(f"\n All numeric columns have correct dtype!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Data Cleaner")
    parser.add_argument("--clean", action="store_true",
                        help="Process raw JSON files into clean CSV")
    args = parser.parse_args()
    if args.clean:
        clean_and_save()
    else:
        print("Use --clean to start.")
        parser.print_help()