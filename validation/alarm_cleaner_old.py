import argparse
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CSV = PROJECT_ROOT / "data" / "raw" / "alarms" / "alarms-240222-010325.csv"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_EVENTS = PROCESSED_DIR / "alarms_clean.csv"

WAR_START = pd.Timestamp("2022-02-24")

EXPECTED_26 = [
    "Vinnytsia Oblast", "Volyn Oblast", "Dnipropetrovsk Oblast",
    "Donetsk Oblast", "Zhytomyr Oblast", "Zakarpattia Oblast",
    "Zaporizhzhia Oblast", "Ivano-Frankivsk Oblast", "Kyiv Oblast",
    "Kirovohrad Oblast", "Luhansk Oblast", "Lviv Oblast",
    "Mykolaiv Oblast", "Odesa Oblast", "Poltava Oblast", "Rivne Oblast",
    "Sumy Oblast", "Ternopil Oblast", "Kharkiv Oblast", "Kherson Oblast",
    "Khmelnytskyi Oblast", "Cherkasy Oblast", "Chernivtsi Oblast",
    "Chernihiv Oblast", "City of Kyiv", "Crimea",
]

FRONTLINE = [
    "Kharkiv Oblast", "Donetsk Oblast", "Sumy Oblast",
    "Zaporizhzhia Oblast", "Kherson Oblast", "Luhansk Oblast",
]

EGGS = ["test", "fake", "easter", "egg", "kurochkin", "homework",
        "placeholder", "bonus", "пасхалка", "тест"]

def hard_map_region(text):
    if pd.isna(text): return None
    t = str(text).lower().strip()
    mapping = [
        (["крим", "crimea"], "Crimea"), (["вінниц"], "Vinnytsia Oblast"),
        (["волин"], "Volyn Oblast"), (["дніпро"], "Dnipropetrovsk Oblast"),
        (["донец", "донеч"], "Donetsk Oblast"), (["житомир"], "Zhytomyr Oblast"),
        (["закарпат"], "Zakarpattia Oblast"), (["запорі"], "Zaporizhzhia Oblast"),
        (["франківс"], "Ivano-Frankivsk Oblast"), (["кіровоград", "кропивн"], "Kirovohrad Oblast"),
        (["луган"], "Luhansk Oblast"), (["львів"], "Lviv Oblast"),
        (["миколаїв"], "Mykolaiv Oblast"), (["одес"], "Odesa Oblast"),
        (["полтав"], "Poltava Oblast"), (["рівнен"], "Rivne Oblast"),
        (["сумськ", "суми"], "Sumy Oblast"), (["терноп"], "Ternopil Oblast"),
        (["харків"], "Kharkiv Oblast"), (["херсон"], "Kherson Oblast"),
        (["хмельниц"], "Khmelnytskyi Oblast"), (["черкас"], "Cherkasy Oblast"),
        (["чернівец", "буковин"], "Chernivtsi Oblast"), (["чернігів"], "Chernihiv Oblast"),
        (["київськ"], "Kyiv Oblast"), (["київ", "kyiv"], "City of Kyiv"),
    ]
    for keys, region in mapping:
        if any(k in t for k in keys): return region
    return None

def process():
    issues = []
    print(f"\n{'=' * 65}\n  ALARMS DATA PROCESSOR\n{'=' * 65}")

    if not RAW_CSV.exists():
        print(f"ERROR: {RAW_CSV} not found")
        sys.exit(1)

    try:
        df = pd.read_csv(RAW_CSV, sep=";")
        if len(df.columns) < 3: df = pd.read_csv(RAW_CSV, sep=",")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    n_raw = len(df)
    print(f"  Loaded: {n_raw:,} rows")

    start_col = "start" if "start" in df.columns else "start_time"
    end_col = "end" if "end" in df.columns else "end_time"
    region_col = "region_city" if "region_city" in df.columns else "region"

    df["region"] = df[region_col].apply(hard_map_region)
    df["start_dt"] = pd.to_datetime(df[start_col], errors="coerce")
    df["end_dt"] = pd.to_datetime(df[end_col], errors="coerce") if end_col else pd.NaT
    df = df.dropna(subset=["region", "start_dt"]).copy()

    df = df[df["start_dt"] >= WAR_START].copy()

    for col in df.select_dtypes("object").columns:
        egg_mask = df[col].astype(str).str.lower().apply(lambda x: any(e in x for e in EGGS))
        if egg_mask.sum():
            issues.append(f"EASTER EGGS: removed {egg_mask.sum()} rows from {col}")
            df = df[~egg_mask]

    if df["end_dt"].notna().any():
        neg_mask = (df["end_dt"] < df["start_dt"]) & df["end_dt"].notna()
        if neg_mask.sum():
            issues.append(f"SWAPPED: fixed {neg_mask.sum()} negative durations")
            df.loc[neg_mask, ["start_dt", "end_dt"]] = df.loc[neg_mask, ["end_dt", "start_dt"]].values

    df["duration_min"] = (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 60
    df["duration_min"] = df["duration_min"].fillna(60)
    df = df[df["duration_min"] > 0].copy()

    n_pre_dupes = len(df)
    df = df.drop_duplicates(subset=["region", "start_dt"], keep="first")
    if n_pre_dupes - len(df):
        issues.append(f"REMOVED: {n_pre_dupes - len(df)} duplicates")

    df["date"] = df["start_dt"].dt.date
    df["hour"] = df["start_dt"].dt.hour
    df["day_of_week"] = df["start_dt"].dt.dayofweek
    df["month"] = df["start_dt"].dt.month
    df["is_frontline"] = df["region"].isin(FRONTLINE).astype(int)
    df["day_name"] = df["start_dt"].dt.day_name()
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["time_of_day"] = df["hour"].apply(
        lambda h: "Night (00-06)" if h < 6
        else "Morning (06-12)" if h < 12
        else "Day (12-18)" if h < 18
        else "Evening (18-24)"
    )
    df["is_night"] = (df["hour"] < 6).astype(int)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_EVENTS, index=False)

    print(f"\n{'='*65}")
    print(f"  REPORT")
    print(f"{'='*65}")
    print(f"  Raw records:  {n_raw:,}")
    print(f"  Clean events: {len(df):,}")
    print(f"  Removed:      {n_raw - len(df):,} ({(n_raw - len(df))/n_raw*100:.2f}%)")
    print(f"  Regions:      {df.region.nunique()}/26")
    print(f"\n  Duration (minutes):")
    print(f"    mean:   {df.duration_min.mean():.1f}")
    print(f"    median: {df.duration_min.median():.1f}")
    print(f"    max:    {df.duration_min.max():.1f}")
    print(f"\n  Events per region:")
    for reg, cnt in df.groupby("region").size().sort_values(ascending=False).items():
        print(f"    {reg:30s}: {cnt:>5,}")
    if issues:
        print(f"\n  Issues ({len(issues)}):")
        for iss in issues:
            print(f"    • {iss}")
    print(f"\n{'='*65}")
    print(f"COMPLETE: {len(df):,} events → {OUTPUT_EVENTS.name}")
    print(f"{'='*65}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--process", action="store_true")
    if p.parse_args().process:
        process()
    else:
        print("Use --process")