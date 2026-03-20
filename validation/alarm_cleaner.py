import argparse
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CSV = PROJECT_ROOT / "data" / "raw" / "alarms" / "alarms-merged.csv"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_CSV = PROCESSED_DIR / "alarms_clean.csv"

WAR_START = pd.Timestamp("2022-02-24")

EXPECTED_REGIONS: set[str] = {
    "Vinnytsia Oblast",       "Volyn Oblast",
    "Dnipropetrovsk Oblast",  "Donetsk Oblast",
    "Zhytomyr Oblast",        "Zakarpattia Oblast",
    "Zaporizhzhia Oblast",    "Ivano-Frankivsk Oblast",
    "Kyiv Oblast",            "City of Kyiv",
    "Kirovohrad Oblast",      "Lviv Oblast",
    "Mykolaiv Oblast",        "Odesa Oblast",
    "Poltava Oblast",         "Rivne Oblast",
    "Sumy Oblast",            "Ternopil Oblast",
    "Kharkiv Oblast",         "Kherson Oblast",
    "Khmelnytskyi Oblast",    "Cherkasy Oblast",
    "Chernivtsi Oblast",      "Chernihiv Oblast",
}

FRONTLINE: set[str] = {
    "Kharkiv Oblast", "Donetsk Oblast", "Sumy Oblast",
    "Zaporizhzhia Oblast", "Kherson Oblast",
}

EASTER_EGGS: list[str] = [
    "test", "fake", "easter", "egg", "курочкін", "kurochkin",
    "тестов", "тест", "placeholder", "bonus", "пасхалк",
    "homework", "dummy", "sample",
]

REGION_CITY_MAP: dict[str, str] = {
    "вінницька обл.":          "Vinnytsia Oblast",
    "волинська обл.":          "Volyn Oblast",
    "дніпропетровська обл.":   "Dnipropetrovsk Oblast",
    "донецька обл.":           "Donetsk Oblast",
    "житомирська обл.":        "Zhytomyr Oblast",
    "закарпатська обл.":       "Zakarpattia Oblast",
    "запорізька обл.":         "Zaporizhzhia Oblast",
    "івано-франківська обл.":  "Ivano-Frankivsk Oblast",
    "київ":                    "City of Kyiv",
    "київська обл.":           "Kyiv Oblast",
    "кіровоградська обл.":     "Kirovohrad Oblast",
    "луганська обл.":          "Luhansk Oblast",
    "львівська обл.":          "Lviv Oblast",
    "миколаївська обл.":       "Mykolaiv Oblast",
    "одеська обл.":            "Odesa Oblast",
    "полтавська обл.":         "Poltava Oblast",
    "рівненська обл.":         "Rivne Oblast",
    "сумська обл.":            "Sumy Oblast",
    "тернопільська обл.":      "Ternopil Oblast",
    "харківська обл.":         "Kharkiv Oblast",
    "херсонська обл.":         "Kherson Oblast",
    "хмельницька обл.":        "Khmelnytskyi Oblast",
    "черкаська обл.":          "Cherkasy Oblast",
    "чернівецька обл.":        "Chernivtsi Oblast",
    "чернігівська обл.":       "Chernihiv Oblast",
    "кримська обл.":           "__drop__",
    "ар крим":                 "__drop__",
    "республіка крим":         "__drop__",
}

def _map_region(val: object) -> str | None:
    if pd.isna(val):
        return None
    result = REGION_CITY_MAP.get(str(val).lower().strip())
    return None if (result is None or result == "__drop__") else result

def process() -> None:
    issues: list[str] = []

    print("=" * 65)
    print("  ALARMS PROCESSOR")
    print("=" * 65)

    if not RAW_CSV.exists():
        print(f"ERROR: {RAW_CSV} not found")
        sys.exit(1)

    print(f"  Source: {RAW_CSV.name}")

    try:
        df = pd.read_csv(
            RAW_CSV,
            sep=";",
            encoding="utf-8-sig",
            dtype={"original_alarms": str},
            low_memory=False,
        )
        if len(df.columns) < 3:
            df = pd.read_csv(
                RAW_CSV,
                sep=",",
                encoding="utf-8-sig",
                dtype={"original_alarms": str},
                low_memory=False,
            )
    except Exception as e:
        print(f"ERROR loading file: {e}")
        sys.exit(1)

    df.columns = [c.lstrip("\ufeff").strip() for c in df.columns]

    n_raw = len(df)
    print(f"  Rows:    {n_raw:,}")
    print(f"  Columns: {df.columns.tolist()}")

    col_start = next(
        (c for c in df.columns if c in ("start", "start_time", "start_dt")), None
    )
    col_end = next(
        (c for c in df.columns if c in ("end", "end_time", "end_dt")), None
    )
    if col_start is None:
        print("ERROR: start column not found")
        sys.exit(1)
    if "region_city" not in df.columns:
        print("ERROR: 'region_city' column missing")
        sys.exit(1)

    text_cols = df.select_dtypes("object").columns.tolist()
    egg_mask = pd.Series(False, index=df.index)
    for col in text_cols:
        col_lower = df[col].astype(str).str.lower()
        for pattern in EASTER_EGGS:
            egg_mask |= col_lower.str.contains(pattern, na=False)
    n_eggs = egg_mask.sum()
    if n_eggs:
        issues.append(f"EASTER EGGS: removed {n_eggs} planted test/fake rows")
        print(f"Easter eggs found and removed: {n_eggs}")
        df = df[~egg_mask].copy()
    else:
        print("No easter egg rows detected")

    df["start_dt"] = pd.to_datetime(df[col_start], errors="coerce")
    df["end_dt"] = (pd.to_datetime(df[col_end], errors="coerce")
                    if col_end else pd.NaT)

    bad_start = df["start_dt"].isna().sum()
    if bad_start:
        issues.append(f"DROPPED {bad_start} — unparseable start_dt")
        df = df.dropna(subset=["start_dt"]).copy()

    df["region"] = df["region_city"].apply(_map_region)
    n_unmapped = df["region"].isna().sum()
    if n_unmapped:
        bad_vals = (df.loc[df["region"].isna(), "region_city"]
                    .value_counts().head(10).to_dict())
        issues.append(f"DROPPED {n_unmapped} — unmapped region_city: {bad_vals}")
    df = df.dropna(subset=["region"]).copy()

    pre = (df["start_dt"] < WAR_START).sum()
    if pre:
        issues.append(f"DROPPED {pre} — before war start ({WAR_START.date()})")
        df = df[df["start_dt"] >= WAR_START].copy()

    df["duration_min"] = (
            (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 60
    )

    data_end = df["start_dt"].max()
    open_mask = df["end_dt"].isna()
    n_open = open_mask.sum()

    if n_open:
        issues.append(
            f"INFO: {n_open} open alarms (ongoing for months/years) "
            f"closed at dataset max time: {data_end.date()}"
        )
        df.loc[open_mask, "end_dt"] = data_end

    df["duration_min"] = (
            (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 60
    )

    bad_dur = (df["duration_min"] <= 0).sum()
    if bad_dur:
        issues.append(f"DROPPED {bad_dur} — duration ≤ 0 min (data errors)")
        df = df[df["duration_min"] > 0].copy()

    long_n = (df["duration_min"] > 1440).sum()
    if long_n:
        issues.append(
            f"INFO: {long_n} alarms > 24h kept (frontline sustained operations, "
            f"max={df['duration_min'].max() / 60 / 24:.1f} days)"
        )

    n_pre = len(df)
    df = df.drop_duplicates(subset=["region", "start_dt"], keep="first")
    dupes = n_pre - len(df)
    if dupes:
        issues.append(f"DROPPED {dupes} duplicate (region, start_dt) rows")

    df["date"] = df["start_dt"].dt.normalize()
    df["hour"] = df["start_dt"].dt.hour
    df["day_of_week"] = df["start_dt"].dt.dayofweek
    df["month"] = df["start_dt"].dt.month
    df["year"] = df["start_dt"].dt.year
    df["day_name"] = df["start_dt"].dt.day_name()
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_frontline"] = df["region"].isin(FRONTLINE).astype(int)
    df["is_night"] = (df["hour"] < 6).astype(int)
    df["time_of_day"] = df["hour"].apply(
        lambda h: "Night (00-06)" if h < 6
        else "Morning (06-12)" if h < 12
        else "Day (12-18)" if h < 18
        else "Evening (18-24)"
    )
    if "all_region" in df.columns:
        df["alarm_source"] = df["all_region"].map(
            {1: "whole_oblast", 0: "merged_from_districts"}
        ).fillna("unknown")

    present = set(df["region"].unique())
    missing_r = EXPECTED_REGIONS - present
    extra_r = present - EXPECTED_REGIONS
    if missing_r:
        issues.append(f"MISSING EXPECTED REGIONS: {sorted(missing_r)}")
    if extra_r:
        issues.append(f"UNEXPECTED EXTRA REGIONS: {sorted(extra_r)}")

    keep = [
        "region", "start_dt", "end_dt", "duration_min",
        "date", "hour", "day_of_week", "day_name",
        "month", "year",
        "is_weekend", "is_frontline", "is_night", "time_of_day",
    ]
    if "alarm_source" in df.columns: keep.append("alarm_source")
    if "original_alarms" in df.columns: keep.append("original_alarms")

    df_out = df[keep].copy()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False)

    by_region = df_out.groupby("region").size().sort_values(ascending=False)
    by_year = df_out.groupby("year").size()
    dur = df_out["duration_min"]

    print(f"\n{'=' * 65}")
    print(f"  REPORT")
    print(f"{'=' * 65}")
    print(f"  Input:    {n_raw:,} rows")
    print(f"  Output:   {len(df_out):,} events")
    print(f"  Removed:  {n_raw - len(df_out):,}  "
          f"({(n_raw - len(df_out)) / n_raw * 100:.2f}%)")
    print(f"  Regions:  {df_out['region'].nunique()} / {len(EXPECTED_REGIONS)}")
    print(f"  Dates:    {df_out['start_dt'].min().date()} → "
          f"{df_out['start_dt'].max().date()}")
    print()
    print(f"  Duration (min):  mean={dur.mean():.1f}  "
          f"median={dur.median():.1f}  "
          f"p95={dur.quantile(0.95):.1f}  "
          f"max={dur.max():.1f}")
    print()
    print(f"  By year:")
    for yr, cnt in by_year.items():
        print(f"    {yr}: {cnt:>6,}")
    print()
    print(f"  By region:")
    for reg, cnt in by_region.items():
        front = "FRONTLINE" if reg in FRONTLINE else ""
        print(f"    {reg:35s}: {cnt:>5,}{front}")
    if "alarm_source" in df_out.columns:
        print()
        print(f"  Alarm source:")
        for src, cnt in df_out["alarm_source"].value_counts().items():
            print(f"    {src}: {cnt:,}")
    if issues:
        print(f"\n  Issues / notes ({len(issues)}):")
        for iss in issues:
            print(f"    • {iss}")
    else:
        print("\n  No issues.")

    print(f"\n{'=' * 65}")
    print(f"  COMPLETE → {OUTPUT_CSV.name}")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process alarms-merged.csv → alarms_clean.csv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--process", action="store_true")
    args = parser.parse_args()
    if args.process:
        process()
    else:
        parser.print_help()