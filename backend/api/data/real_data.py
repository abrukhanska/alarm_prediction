import argparse
import json
import pickle
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"
MODELS_DIR   = PROJECT_ROOT / "models"
KYIV_TZ      = ZoneInfo("Europe/Kyiv")

INFERENCE_CACHE      = PROCESSED / "inference_cache.csv"
METADATA_PATH        = MODELS_DIR / "retrain_metadata.json"
LATEST_JSON          = PROJECT_ROOT / "data" / "predictions" / "latest.json"
_BOOTSTRAP_THRESHOLD = 0.612

WEATHER_TTL = 60 * 60
ALARMS_TTL  = 3  * 60
CACHE_TTL   = 60 * 60
MODEL_TTL   = 10 * 60

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
    range(0,  20):  "safe",
    range(20, 40):  "low",
    range(40, 60):  "medium",
    range(60, 80):  "high",
    range(80, 101): "critical",
}

COLS_TO_REMOVE = [
    "region", "datetime_hour", "alarm", "n_regions_alarm",
    "n_regions_alarm_lag_1h", "n_regions_alarm_lag_2h",
    "n_regions_alarm_lag_3h", "n_regions_alarm_momentum",
    "alarm_lag_1h", "alarm_lag_2h", "alarm_lag_3h",
]

WEATHER_COLS = [
    "city_address", "datetime_hour",
    "hour_temp", "hour_humidity", "hour_windspeed", "hour_winddir",
    "hour_visibility", "hour_cloudcover", "hour_pressure", "hour_precip",
]

_cache: dict = {
    "alarms":  {"data": None, "at": 0.0},
    "weather": {"data": None, "at": 0.0},
    "model":   {"data": None, "at": 0.0, "threshold": _BOOTSTRAP_THRESHOLD},
    "icache":  {"data": None, "at": 0.0},
}

def _stale(key: str, ttl: float) -> bool:
    return (time.time() - _cache[key]["at"]) > ttl

def _alarms() -> pd.DataFrame:
    if _cache["alarms"]["data"] is None or _stale("alarms", ALARMS_TTL):
        p  = PROCESSED / "alarms_clean.csv"
        df = pd.read_csv(p, parse_dates=["start_dt", "end_dt"]) if p.exists() else pd.DataFrame()
        _cache["alarms"]["data"] = df
        _cache["alarms"]["at"]   = time.time()
    return _cache["alarms"]["data"]

def _weather() -> pd.DataFrame:
    if _cache["weather"]["data"] is None or _stale("weather", WEATHER_TTL):
        p = PROCESSED / "weather_clean.csv"
        if p.exists():
            avail = pd.read_csv(p, nrows=0).columns.tolist()
            cols  = [c for c in WEATHER_COLS if c in avail]
            df    = pd.read_csv(p, usecols=cols, parse_dates=["datetime_hour"], low_memory=False)
        else:
            df = pd.DataFrame(columns=WEATHER_COLS)
        _cache["weather"]["data"] = df
        _cache["weather"]["at"]   = time.time()
    return _cache["weather"]["data"]

def _model():
    if _cache["model"]["data"] is None or _stale("model", MODEL_TTL):
        mp = MODELS_DIR / "4__lightgbm__v1.pkl"
        if not mp.exists():
            raise FileNotFoundError(f"Model not found: {mp}")
        with open(mp, "rb") as f:
            model = pickle.load(f)
        thr = _BOOTSTRAP_THRESHOLD
        if METADATA_PATH.exists():
            try:
                with open(METADATA_PATH, encoding="utf-8") as mf:
                    thr = float(json.load(mf).get("last_threshold", thr))
            except Exception:
                pass
        _cache["model"]["data"]      = model
        _cache["model"]["threshold"] = thr
        _cache["model"]["at"]        = time.time()
    return _cache["model"]["data"], _cache["model"]["threshold"]

def _icache() -> pd.DataFrame:
    if _cache["icache"]["data"] is None or _stale("icache", CACHE_TTL):
        if not INFERENCE_CACHE.exists():
            raise FileNotFoundError(
                "inference_cache.csv not found. "
                "Run: python backend/api/data/real_data.py --prepare"
            )
        df = pd.read_csv(INFERENCE_CACHE, parse_dates=["datetime_hour"])
        _cache["icache"]["data"] = df
        _cache["icache"]["at"]   = time.time()
    return _cache["icache"]["data"]

def _read_latest_json() -> dict | None:
    if not LATEST_JSON.exists():
        return None
    try:
        with open(LATEST_JSON, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _prob_to_threat(prob: float) -> str:
    pct = int(prob * 100)
    for r, level in THREAT_LEVELS.items():
        if pct in r:
            return level
    return "medium"

def get_stats() -> dict:
    df  = _alarms()
    now = datetime.now(KYIV_TZ).replace(tzinfo=None)

    if df.empty or "start_dt" not in df.columns:
        return {
            "active_alarms_count": 0, "total_regions": 25,
            "avg_threat_level":    0.0, "most_threatened_region": "none",
            "total_alarms_today":  0,  "total_duration_today_hours": 0.0,
        }

    active       = df[(df["start_dt"] <= now) & (df["end_dt"] >= now)]
    today_alarms = df[df["start_dt"].dt.date == now.date()]
    dur  = today_alarms["duration_min"].sum() / 60 if "duration_min" in today_alarms.columns else 0.0
    most = "none"
    if not active.empty and "region" in active.columns:
        most = active.groupby("region").size().idxmax()

    return {
        "active_alarms_count":        int(len(active)),
        "total_regions":              25,
        "avg_threat_level":           round(len(active) / 25 * 10, 1),
        "most_threatened_region":     most,
        "total_alarms_today":         int(len(today_alarms)),
        "total_duration_today_hours": round(float(dur), 1),
    }

def get_weather(region_slug: str) -> dict:
    df          = _weather()
    region_name = REGION_SLUG_MAP.get(region_slug.lower(), region_slug)
    city        = REGION_TO_CITY.get(region_name, "Kyiv")

    city_df = df[df["city_address"] == city]
    if city_df.empty:
        city_df = df[df["city_address"].str.contains(city, na=False, case=False)]
    if city_df.empty:
        city_df = df[df["city_address"] == "Kyiv"]

    if city_df.empty:
        return {
            "region": region_slug, "temp": 0.0, "humidity": 0.0, "windspeed": 0.0,
            "winddir": 0.0, "visibility": 10.0, "cloudcover": 0.0,
            "pressure": 1013.0, "conditions": "Unknown", "precip": 0.0,
        }

    r = city_df.sort_values("datetime_hour").iloc[-1]
    return {
        "region":     region_slug,
        "temp":       float(r.get("hour_temp",       0)    or 0),
        "humidity":   float(r.get("hour_humidity",   0)    or 0),
        "windspeed":  float(r.get("hour_windspeed",  0)    or 0),
        "winddir":    float(r.get("hour_winddir",    0)    or 0),
        "visibility": float(r.get("hour_visibility", 10)   or 10),
        "cloudcover": float(r.get("hour_cloudcover", 0)    or 0),
        "pressure":   float(r.get("hour_pressure",   1013) or 1013),
        "conditions": "Clear",
        "precip":     float(r.get("hour_precip",     0)    or 0),
    }

def get_current_alarms() -> dict:
    df_alarms = _alarms()
    df_cache  = _icache()
    model, _  = _model()
    now       = datetime.now(KYIV_TZ).replace(tzinfo=None)

    active_regions: set = set()
    if not df_alarms.empty and "start_dt" in df_alarms.columns:
        active_regions = set(
            df_alarms[(df_alarms["start_dt"] <= now) & (df_alarms["end_dt"] >= now)]["region"]
        )

    region_probs: dict = {}
    if not df_cache.empty and "region" in df_cache.columns:
        latest = df_cache.sort_values("datetime_hour").groupby("region").tail(1)
        drop   = [c for c in COLS_TO_REMOVE if c in latest.columns]
        try:
            probs        = model.predict_proba(latest.drop(columns=drop).fillna(0))[:, 1]
            region_probs = dict(zip(latest["region"], probs))
        except Exception as e:
            print(f"[alarms] predict failed: {e}")

    out = []
    for slug, region_name in REGION_SLUG_MAP.items():
        is_active = region_name in active_regions
        prob      = float(region_probs.get(region_name, 0.2))
        out.append({
            "id":     slug,
            "name":   region_name.replace(" Oblast", "").replace("City of ", ""),
            "active": is_active,
            "type":         "active" if is_active else "none",
            "since":        now.isoformat() + "Z" if is_active else None,
            "threat_level": "critical" if is_active else _prob_to_threat(prob),
        })

    return {
        "timestamp":     now.isoformat() + "Z",
        "active_count":  len(active_regions),
        "total_regions": 25,
        "regions":       out,
    }

def get_prediction(region_slug: str) -> dict:
    region_name = REGION_SLUG_MAP.get(region_slug.lower(), region_slug)
    now         = datetime.now(KYIV_TZ).replace(tzinfo=None)

    res: dict = {
        "region":        region_slug,
        "region_name":   region_name,
        "threat_level":  "safe",
        "probability_1h":  0.0,
        "probability_3h":  0.0,
        "probability_6h":  0.0,
        "probability_12h": 0.0,
        "threat_types": {"missile": 0.0, "drone": 0.0, "artillery": 0.0},
        "updated_at":   now.isoformat() + "Z",
        "forecast":     {},
    }

    pred_data = _read_latest_json()
    if not pred_data:
        return res

    probs = pred_data.get("regions_probabilities", {}).get(region_name, {})
    if probs:
        prob_values = list(probs.values())

        res["probability_1h"]  = round(float(prob_values[0]),  3) if len(prob_values) >= 1  else 0.0
        res["probability_3h"]  = round(float(prob_values[2]),  3) if len(prob_values) >= 3  else 0.0
        res["probability_6h"]  = round(float(prob_values[5]),  3) if len(prob_values) >= 6  else 0.0
        res["probability_12h"] = round(float(prob_values[11]), 3) if len(prob_values) >= 12 else 0.0

        res["threat_level"] = _prob_to_threat(res["probability_1h"])
        res["updated_at"]   = pred_data.get("last_prediction_time", res["updated_at"])

    forecast = pred_data.get("regions_forecast", {}).get(region_name, {})
    res["forecast"] = forecast

    return res


def get_timeline(region_slug: str) -> dict:
    region_name = REGION_SLUG_MAP.get(region_slug.lower(), region_slug)
    pred_data   = _read_latest_json()

    if not pred_data:
        return {"region": region_slug, "hours": []}

    probs    = pred_data.get("regions_probabilities", {}).get(region_name, {})
    forecast = pred_data.get("regions_forecast",      {}).get(region_name, {})

    if not probs:
        return {"region": region_slug, "hours": []}

    hours_out = []
    for hour_label, prob in probs.items():
        p = float(prob)
        hours_out.append({
            "hour":        hour_label,
            "probability": round(p, 3),
            "missile":    round(p * 0.40, 3),
            "drone":      round(p * 0.45, 3),
            "artillery":  round(p * 0.15, 3),
            "alarm": bool(forecast.get(hour_label, False)),
        })

    return {"region": region_slug, "hours": hours_out}

def prepare_inference_cache() -> None:
    src = PROCESSED / "features_dataset.csv"
    if not src.exists():
        print(f"ERROR: {src} not found")
        return
    print(f"Reading {src} in chunks ...")
    last_rows: dict = {}
    for chunk in pd.read_csv(src, chunksize=50_000, low_memory=False):
        if "region" not in chunk.columns:
            print("ERROR: 'region' column missing. Re-run feature_engineering.py --build")
            return
        for region, grp in chunk.groupby("region"):
            last_rows[region] = (
                pd.concat([last_rows[region], grp]).tail(24)
                if region in last_rows else grp.tail(24)
            )
    if not last_rows:
        print("ERROR: no data")
        return
    cache = pd.concat(last_rows.values(), ignore_index=True)
    cache.to_csv(INFERENCE_CACHE, index=False)
    print(
        f"Saved: {INFERENCE_CACHE}  "
        f"({len(cache)} rows, {cache['region'].nunique()} regions, "
        f"{INFERENCE_CACHE.stat().st_size // 1024} KB)"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare", action="store_true",
                        help="Rebuild inference_cache.csv from features_dataset.csv")
    args = parser.parse_args()
    if args.prepare:
        prepare_inference_cache()
    else:
        parser.print_help()