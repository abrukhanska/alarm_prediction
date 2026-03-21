import argparse
import pickle
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent.parent
PROCESSED      = PROJECT_ROOT / "data" / "processed"
MODELS_DIR     = PROJECT_ROOT / "models"
KYIV_TZ        = ZoneInfo("Europe/Kyiv")

INFERENCE_CACHE = PROCESSED / "inference_cache.csv"

REGION_TO_CITY = {
    "Kharkiv Oblast":         "Kharkiv",
    "Donetsk Oblast":         "Donetsk",
    "Zaporizhzhia Oblast":    "Zaporozhye",
    "Sumy Oblast":            "Sumy",
    "Kherson Oblast":         "Kherson",
    "City of Kyiv":           "Kyiv",
    "Kyiv Oblast":            "Kyiv",
    "Dnipropetrovsk Oblast":  "Dnipro",
    "Mykolaiv Oblast":        "Mykolaiv",
    "Odesa Oblast":           "Odesa",
    "Lviv Oblast":            "Lviv",
    "Poltava Oblast":         "Poltava",
    "Chernihiv Oblast":       "Chernihiv",
    "Cherkasy Oblast":        "Cherkasy",
    "Vinnytsia Oblast":       "Vinnytsia",
    "Zhytomyr Oblast":        "Zhytomyr",
    "Rivne Oblast":           "Rivne",
    "Volyn Oblast":           "Lutsk",
    "Ivano-Frankivsk Oblast": "Ivano-Frankivsk",
    "Ternopil Oblast":        "Ternopil",
    "Khmelnytskyi Oblast":    "Khmelnytskyi",
    "Chernivtsi Oblast":      "Chernivtsi",
    "Zakarpattia Oblast":     "Uzhgorod",
    "Kirovohrad Oblast":      "Kropyvnytskyi",
    "Luhansk Oblast":         "Luhansk",
}

REGION_SLUG_MAP = {
    "kharkiv":         "Kharkiv Oblast",
    "donetsk":         "Donetsk Oblast",
    "zaporizhzhia":    "Zaporizhzhia Oblast",
    "sumy":            "Sumy Oblast",
    "kherson":         "Kherson Oblast",
    "kyiv":            "City of Kyiv",
    "kyiv_oblast":     "Kyiv Oblast",
    "dnipropetrovsk":  "Dnipropetrovsk Oblast",
    "luhansk":         "Luhansk Oblast",
    "mykolaiv":        "Mykolaiv Oblast",
    "odesa":           "Odesa Oblast",
    "lviv":            "Lviv Oblast",
    "poltava":         "Poltava Oblast",
    "chernihiv":       "Chernihiv Oblast",
    "cherkasy":        "Cherkasy Oblast",
    "vinnytsia":       "Vinnytsia Oblast",
    "zhytomyr":        "Zhytomyr Oblast",
    "rivne":           "Rivne Oblast",
    "volyn":           "Volyn Oblast",
    "ivano_frankivsk": "Ivano-Frankivsk Oblast",
    "ternopil":        "Ternopil Oblast",
    "khmelnytskyi":    "Khmelnytskyi Oblast",
    "chernivtsi":      "Chernivtsi Oblast",
    "zakarpattia":     "Zakarpattia Oblast",
    "kirovohrad":      "Kirovohrad Oblast",
}

THREAT_LEVELS = {
    range(0,  20): "safe",
    range(20, 40): "low",
    range(40, 60): "medium",
    range(60, 80): "high",
    range(80, 101): "critical",
}

COLS_TO_REMOVE = [
    'region', 'datetime_hour', 'alarm', 'n_regions_alarm',
    'n_regions_alarm_lag_1h', 'n_regions_alarm_lag_2h',
    'n_regions_alarm_lag_3h', 'n_regions_alarm_momentum',
    'alarm_lag_1h', 'alarm_lag_2h', 'alarm_lag_3h',
]

WEATHER_COLS = [
    "city_address", "datetime_hour",
    "hour_temp", "hour_humidity", "hour_windspeed", "hour_winddir",
    "hour_visibility", "hour_cloudcover", "hour_pressure", "hour_precip",
]

_alarms_df  = None
_weather_df = None
_lgbm_model = None
_cache_df   = None

def prepare_inference_cache() -> None:
    src = PROCESSED / "features_dataset.csv"
    if not src.exists():
        print(f"ERROR: {src} not found")
        return

    print(f"Reading {src} in chunks (не в RAM одразу)...")
    chunk_size = 50_000
    last_rows: dict[str, pd.DataFrame] = {}

    for chunk in pd.read_csv(src, chunksize=chunk_size, low_memory=False):
        if "region" not in chunk.columns:
            continue
        for region, grp in chunk.groupby("region"):
            if region in last_rows:
                last_rows[region] = pd.concat([last_rows[region], grp]).tail(24)
            else:
                last_rows[region] = grp.tail(24)

    if not last_rows:
        print("ERROR: no data found")
        return

    cache = pd.concat(last_rows.values(), ignore_index=True)
    cache.to_csv(INFERENCE_CACHE, index=False)
    size_kb = INFERENCE_CACHE.stat().st_size / 1024
    print(f"Cache saved: {INFERENCE_CACHE}")
    print(f"  Rows: {len(cache)} ({cache['region'].nunique()} regions × ~24 rows)")
    print(f"  Size: {size_kb:.0f} KB  (замість ~2.5 GB!)")

def _prob_to_threat(prob: float) -> str:
    pct = int(prob * 100)
    for r, level in THREAT_LEVELS.items():
        if pct in r:
            return level
    return "medium"

def _load_all() -> None:
    global _alarms_df, _weather_df, _lgbm_model, _cache_df

    if _alarms_df is None:
        _alarms_df = pd.read_csv(
            PROCESSED / "alarms_clean.csv",
            parse_dates=["start_dt", "end_dt"],
        )

    if _weather_df is None:
        available = pd.read_csv(
            PROCESSED / "weather_clean.csv", nrows=0
        ).columns.tolist()
        usecols = [c for c in WEATHER_COLS if c in available]
        _weather_df = pd.read_csv(
            PROCESSED / "weather_clean.csv",
            usecols=usecols,
            parse_dates=["datetime_hour"],
            low_memory=False,
        )

    if _lgbm_model is None:
        with open(MODELS_DIR / "lightgbm_model.pkl", "rb") as f:
            _lgbm_model = pickle.load(f)

    if _cache_df is None:
        if not INFERENCE_CACHE.exists():
            raise FileNotFoundError(
                f"inference_cache.csv not found!\n"
                f"Run first: python backend/api/data/real_data.py --prepare"
            )
        _cache_df = pd.read_csv(INFERENCE_CACHE, parse_dates=["datetime_hour"])

def get_stats() -> dict:
    _load_all()
    now   = datetime.now(KYIV_TZ).replace(tzinfo=None)
    today = now.date()

    active       = _alarms_df[(_alarms_df["start_dt"] <= now) & (_alarms_df["end_dt"] >= now)]
    today_alarms = _alarms_df[_alarms_df["start_dt"].dt.date == today]
    total_dur    = today_alarms["duration_min"].sum() / 60

    return {
        "active_alarms_count":        len(active),
        "total_regions":              25,
        "avg_threat_level":           round(len(active) / 25 * 10, 1),
        "most_threatened_region":     (
            active.groupby("region").size().idxmax()
            if not active.empty else "none"
        ),
        "total_alarms_today":         len(today_alarms),
        "total_duration_today_hours": round(total_dur, 1),
    }

def get_weather(region_slug: str) -> dict:
    _load_all()
    region_name = REGION_SLUG_MAP.get(region_slug, region_slug)
    city        = REGION_TO_CITY.get(region_name, "Kyiv")

    city_df = _weather_df[_weather_df["city_address"] == city]
    if city_df.empty:
        city_df = _weather_df[_weather_df["city_address"] == "Kyiv"]

    last = city_df.sort_values("datetime_hour").iloc[-1]

    return {
        "region":     region_slug,
        "temp":       float(last.get("hour_temp",       0) or 0),
        "humidity":   float(last.get("hour_humidity",   0) or 0),
        "windspeed":  float(last.get("hour_windspeed",  0) or 0),
        "winddir":    float(last.get("hour_winddir",    0) or 0),
        "visibility": float(last.get("hour_visibility", 10) or 10),
        "cloudcover": float(last.get("hour_cloudcover", 0) or 0),
        "pressure":   float(last.get("hour_pressure",   1013) or 1013),
        "conditions": "Clear",
        "precip":     float(last.get("hour_precip",     0) or 0),
    }

def get_current_alarms() -> dict:
    _load_all()
    now = datetime.now(KYIV_TZ).replace(tzinfo=None)

    active_regions = set(
        _alarms_df[
            (_alarms_df["start_dt"] <= now) &
            (_alarms_df["end_dt"] >= now)
            ]["region"].tolist()
    )

    latest_features = _cache_df.sort_values("datetime_hour").groupby("region").tail(1)

    drop_cols = [c for c in COLS_TO_REMOVE if c in latest_features.columns]
    X_batch = latest_features.drop(columns=drop_cols).fillna(0)

    try:
        probs = _lgbm_model.predict_proba(X_batch)[:, 1]
        region_probs = dict(zip(latest_features["region"], probs))
    except Exception:
        region_probs = {}

    regions_out = []
    for slug, region_name in REGION_SLUG_MAP.items():
        is_active = region_name in active_regions

        prob = float(region_probs.get(region_name, 0.2))
        calculated_threat = _prob_to_threat(prob)

        regions_out.append({
            "id": slug,
            "name": region_name.replace(" Oblast", "").replace("City of ", ""),
            "active": is_active,
            "type": "missile" if is_active else "none",
            "since": now.isoformat() + "Z" if is_active else None,
            "threat_level": "critical" if is_active else calculated_threat,
            "probability": round(prob * 100, 1)
        })

    return {
        "timestamp": now.isoformat() + "Z",
        "active_count": len(active_regions),
        "total_regions": 25,
        "regions": regions_out,
    }

def get_prediction(region_slug: str) -> dict:
    _load_all()
    region_name = REGION_SLUG_MAP.get(region_slug, region_slug)
    now         = datetime.now(KYIV_TZ).replace(tzinfo=None)

    region_df = _cache_df[_cache_df["region"] == region_name]
    if region_df.empty:
        prob_1h = 0.2
    else:
        last_row  = region_df.sort_values("datetime_hour").iloc[[-1]]
        drop_cols = [c for c in COLS_TO_REMOVE if c in last_row.columns]
        X = last_row.drop(columns=drop_cols).fillna(0)
        try:
            prob_1h = float(_lgbm_model.predict_proba(X)[0][1])
        except Exception:
            prob_1h = 0.2

    return {
        "region":          region_slug,
        "region_name":     region_name,
        "threat_level":    _prob_to_threat(prob_1h),
        "probability_1h":  round(prob_1h,        3),
        "probability_3h":  round(prob_1h * 0.75, 3),
        "probability_6h":  round(prob_1h * 0.50, 3),
        "probability_12h": round(prob_1h * 0.30, 3),
        "threat_types":    {"missile": 0.4, "drone": 0.45, "artillery": 0.15},
        "updated_at":      now.isoformat() + "Z",
    }

def get_timeline(region_slug: str) -> dict:
    _load_all()
    region_name = REGION_SLUG_MAP.get(region_slug, region_slug)

    region_df = _cache_df[_cache_df["region"] == region_name]
    last_24   = region_df.sort_values("datetime_hour").tail(24)
    drop_cols = [c for c in COLS_TO_REMOVE if c in last_24.columns]

    X_batch = last_24.drop(columns=drop_cols).fillna(0)
    try:
        probs = _lgbm_model.predict_proba(X_batch)[:, 1]
    except Exception:
        probs = [0.2] * len(last_24)

    hours_out = []
    for i, (_, row) in enumerate(last_24.iterrows()):
        prob = float(probs[i])
        hours_out.append({
            "hour":        row["datetime_hour"].strftime("%H:00"),
            "probability": round(prob,        3),
            "missile":     round(prob * 0.40, 3),
            "drone":       round(prob * 0.45, 3),
            "artillery":   round(prob * 0.15, 3),
        })

    return {"region": region_slug, "hours": hours_out}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare", action="store_true",
                        help="Create inference_cache.csv from features_dataset.csv")
    args = parser.parse_args()
    if args.prepare:
        prepare_inference_cache()
    else:
        parser.print_help()