import json
import time
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PROCESSED = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"

LATEST_JSON = PROJECT_ROOT / "data" / "predictions" / "latest.json"
LIVE_STATE_JSON = PROJECT_ROOT / "data" / "live" / "live_state.json"
WEATHER_TODAY_JSON = PROJECT_ROOT / "data" / "live" / "weather_today.json"

KYIV_TZ = ZoneInfo("Europe/Kyiv")

WEATHER_TTL = 60 * 60  # 1 h  — static parquet weather
ALARMS_TTL = 3 * 60  # 3 min — live alarm state
PRED_TTL = 10 * 60  # 10 min — predictions
WTODAY_TTL = 30 * 60  # 30 min — weather_today.json (refreshed hourly by cron)

REGION_TO_CITY = {
    "Kharkiv Oblast": "Kharkiv",
    "Donetsk Oblast": "Donetsk",
    "Zaporizhzhia Oblast": "Zaporozhye",
    "Sumy Oblast": "Sumy",
    "Kherson Oblast": "Kherson",
    "City of Kyiv": "Kyiv",
    "Kyiv Oblast": "Kyiv",
    "Dnipropetrovsk Oblast": "Dnipro",
    "Mykolaiv Oblast": "Mykolaiv",
    "Odesa Oblast": "Odesa",
    "Lviv Oblast": "Lviv",
    "Poltava Oblast": "Poltava",
    "Chernihiv Oblast": "Chernihiv",
    "Cherkasy Oblast": "Cherkasy",
    "Vinnytsia Oblast": "Vinnytsia",
    "Zhytomyr Oblast": "Zhytomyr",
    "Rivne Oblast": "Rivne",
    "Volyn Oblast": "Lutsk",
    "Ivano-Frankivsk Oblast": "Ivano-Frankivsk",
    "Ternopil Oblast": "Ternopil",
    "Khmelnytskyi Oblast": "Khmelnytskyi",
    "Chernivtsi Oblast": "Chernivtsi",
    "Zakarpattia Oblast": "Uzhgorod",
    "Kirovohrad Oblast": "Kropyvnytskyi",
    "Luhansk Oblast": "Luhansk",
}

REGION_SLUG_MAP = {
    "kharkiv": "Kharkiv Oblast",
    "donetsk": "Donetsk Oblast",
    "zaporizhzhia": "Zaporizhzhia Oblast",
    "sumy": "Sumy Oblast",
    "kherson": "Kherson Oblast",
    "kyiv": "City of Kyiv",
    "kyiv_oblast": "Kyiv Oblast",
    "dnipropetrovsk": "Dnipropetrovsk Oblast",
    "luhansk": "Luhansk Oblast",
    "mykolaiv": "Mykolaiv Oblast",
    "odesa": "Odesa Oblast",
    "lviv": "Lviv Oblast",
    "poltava": "Poltava Oblast",
    "chernihiv": "Chernihiv Oblast",
    "cherkasy": "Cherkasy Oblast",
    "vinnytsia": "Vinnytsia Oblast",
    "zhytomyr": "Zhytomyr Oblast",
    "rivne": "Rivne Oblast",
    "volyn": "Volyn Oblast",
    "ivano_frankivsk": "Ivano-Frankivsk Oblast",
    "ternopil": "Ternopil Oblast",
    "khmelnytskyi": "Khmelnytskyi Oblast",
    "chernivtsi": "Chernivtsi Oblast",
    "zakarpattia": "Zakarpattia Oblast",
    "kirovohrad": "Kirovohrad Oblast",
}

REGION_NAME_TO_SLUG: dict[str, str] = {v: k for k, v in REGION_SLUG_MAP.items()}

THREAT_LEVELS = {
    range(0, 20): "safe",
    range(20, 40): "low",
    range(40, 60): "medium",
    range(60, 80): "high",
    range(80, 101): "critical",
}

WEATHER_COLS = [
    "city_address", "datetime_hour",
    "hour_temp", "hour_humidity", "hour_windspeed", "hour_winddir",
    "hour_visibility", "hour_cloudcover", "hour_pressure", "hour_precip",
]

_cache: dict = {
    "alarms": {"data": None, "at": 0.0},
    "live": {"data": None, "at": 0.0},
    "weather": {"data": None, "at": 0.0},
    "wtoday": {"data": None, "at": 0.0},
    "pred": {"data": None, "at": 0.0},
}

def _stale(key: str, ttl: float) -> bool:
    return (time.time() - _cache[key]["at"]) > ttl

def _alarms() -> pd.DataFrame:
    if _cache["alarms"]["data"] is None or _stale("alarms", ALARMS_TTL):
        p = PROCESSED / "alarms_clean.parquet"
        df = pd.read_parquet(p) if p.exists() else pd.DataFrame()
        if "start_dt" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["start_dt"]):
            df["start_dt"] = pd.to_datetime(df["start_dt"])
        if "end_dt" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["end_dt"]):
            df["end_dt"] = pd.to_datetime(df["end_dt"])
        _cache["alarms"]["data"] = df
        _cache["alarms"]["at"]   = time.time()
    return _cache["alarms"]["data"]

def _live_state() -> dict | None:
    if _cache["live"]["data"] is None or _stale("live", ALARMS_TTL):
        if not LIVE_STATE_JSON.exists():
            return None
        try:
            with open(LIVE_STATE_JSON, encoding="utf-8") as f:
                data = json.load(f)
            _cache["live"]["data"] = data
            _cache["live"]["at"]   = time.time()
        except Exception:
            return None
    return _cache["live"]["data"]

def _weather_today() -> dict | None:
    if _cache["wtoday"]["data"] is None or _stale("wtoday", WTODAY_TTL):
        if not WEATHER_TODAY_JSON.exists():
            return None
        try:
            with open(WEATHER_TODAY_JSON, encoding="utf-8") as f:
                data = json.load(f)
            _cache["wtoday"]["data"] = data
            _cache["wtoday"]["at"]   = time.time()
        except Exception:
            return None
    return _cache["wtoday"]["data"]

def _weather_static() -> pd.DataFrame:
    if _cache["weather"]["data"] is None or _stale("weather", WEATHER_TTL):
        p = PROCESSED / "weather_clean.parquet"
        if p.exists():
            df = pd.read_parquet(p, columns=WEATHER_COLS)
            if not pd.api.types.is_datetime64_any_dtype(df["datetime_hour"]):
                df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])
        else:
            df = pd.DataFrame(columns=WEATHER_COLS)
        _cache["weather"]["data"] = df
        _cache["weather"]["at"]   = time.time()
    return _cache["weather"]["data"]

def _read_latest_json() -> dict | None:
    if _cache["pred"]["data"] is None or _stale("pred", PRED_TTL):
        if not LATEST_JSON.exists():
            return None
        try:
            with open(LATEST_JSON, encoding="utf-8") as f:
                data = json.load(f)
            _cache["pred"]["data"] = data
            _cache["pred"]["at"]   = time.time()
        except Exception:
            return None
    return _cache["pred"]["data"]

def _prob_to_threat(prob: float) -> str:
    pct = int(prob * 100)
    for r, level in THREAT_LEVELS.items():
        if pct in r:
            return level
    return "medium"

def _active_regions_from_live() -> set[str]:
    live = _live_state()
    if not live:
        return set()
    return {region for region, is_active in live.get("regions", {}).items() if is_active}

def get_stats() -> dict:
    now = datetime.now(KYIV_TZ).replace(tzinfo=None)
    live = _live_state()
    if live:
        active_count        = int(live.get("active_alarms_count", 0))
        active_region_names = [r for r, v in live.get("regions", {}).items() if v]
        most = active_region_names[0] if active_region_names else "none"
    else:
        df = _alarms()
        if df.empty or "start_dt" not in df.columns:
            return {
                "active_alarms_count":        0,
                "total_regions":              25,
                "avg_threat_level":           0.0,
                "most_threatened_region":     "none",
                "total_alarms_today":         0,
                "total_duration_today_hours": 0.0,
            }
        active = df[(df["start_dt"] <= now) & (df["end_dt"] >= now)]
        active_count = len(active)
        most = (active.groupby("region").size().idxmax()
                if not active.empty and "region" in active.columns else "none")

    df = _alarms()
    if not df.empty and "start_dt" in df.columns:
        today_alarms = df[df["start_dt"].dt.date == now.date()]
        total_today  = int(len(today_alarms))
        dur          = (today_alarms["duration_min"].sum() / 60
                        if "duration_min" in today_alarms.columns else 0.0)
    else:
        total_today = 0
        dur         = 0.0

    return {
        "active_alarms_count":        active_count,
        "total_regions":              25,
        "avg_threat_level":           round(active_count / 25 * 10, 1),
        "most_threatened_region":     most,
        "total_alarms_today":         total_today,
        "total_duration_today_hours": round(float(dur), 1),
    }

def get_weather(region_slug: str, hour: str | None = None) -> dict:
    region_name = REGION_SLUG_MAP.get(region_slug.lower(), region_slug)
    wtoday = _weather_today()
    if wtoday:
        region_hours = wtoday.get("regions", {}).get(region_name, {})
        if region_hours:
            if hour and hour in region_hours:
                w = region_hours[hour]
            else:
                current_hhmm = datetime.now(KYIV_TZ).strftime("%H:00")
                w = region_hours.get(current_hhmm) or next(iter(region_hours.values()))

            return {
                "region": region_slug,
                "temp": float(w.get("temp", 0) or 0),
                "humidity": float(w.get("humidity", 0) or 0),
                "windspeed": float(w.get("windspeed", 0) or 0),
                "winddir": float(w.get("winddir", 0) or 0),
                "visibility": float(w.get("visibility", 10) or 10),
                "cloudcover": float(w.get("cloudcover", 0) or 0),
                "pressure": float(w.get("pressure", 1013) or 1013),
                "conditions": str(w.get("conditions", "Clear") or "Clear"),
                "precip": float(w.get("precip", 0) or 0),
                "source": "live_forecast",  # debug field
            }
    df = _weather_static()
    city = REGION_TO_CITY.get(region_name, "Kyiv")

    if df.empty:
        return _empty_weather(region_slug)

    city_df = df[df["city_address"] == city]
    if city_df.empty:
        city_df = df[df["city_address"].str.contains(city, na=False, case=False)]
    if city_df.empty:
        city_df = df[df["city_address"] == "Kyiv"]
    if city_df.empty:
        return _empty_weather(region_slug)

    r = city_df.sort_values("datetime_hour").iloc[-1]
    return {
        "region": region_slug,
        "temp": float(r.get("hour_temp", 0) or 0),
        "humidity": float(r.get("hour_humidity", 0) or 0),
        "windspeed": float(r.get("hour_windspeed", 0) or 0),
        "winddir": float(r.get("hour_winddir", 0) or 0),
        "visibility": float(r.get("hour_visibility", 10) or 10),
        "cloudcover": float(r.get("hour_cloudcover", 0) or 0),
        "pressure": float(r.get("hour_pressure", 1013) or 1013),
        "conditions": "Clear",
        "precip": float(r.get("hour_precip", 0) or 0),
        "source": "static_parquet",
    }

def _empty_weather(region_slug: str) -> dict:
    return {
        "region": region_slug, "temp": 0.0, "humidity": 0.0,
        "windspeed": 0.0, "winddir": 0.0, "visibility": 10.0,
        "cloudcover": 0.0, "pressure": 1013.0,
        "conditions": "Unknown", "precip": 0.0, "source": "empty",
    }

def get_current_alarms() -> dict:
    now = datetime.now(KYIV_TZ).replace(tzinfo=None)

    active_regions = _active_regions_from_live()

    region_probs: dict[str, float] = {}
    pred_data = _read_latest_json()
    if pred_data:
        for region_name, hour_probs in pred_data.get("regions_probabilities", {}).items():
            if hour_probs:
                region_probs[region_name] = float(list(hour_probs.values())[0])

    out = []
    for slug, region_name in REGION_SLUG_MAP.items():
        is_active = region_name in active_regions
        prob      = region_probs.get(region_name, 0.2)
        out.append({
            "id":           slug,
            "name":         region_name.replace(" Oblast", "").replace("City of ", ""),
            "active":       is_active,
            "type":         "active" if is_active else "none",
            "since":        now.isoformat() + "Z" if is_active else None,
            "threat_level": "critical" if is_active else _prob_to_threat(prob),
            "probability":  round(prob, 3),
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
        "region":          region_slug,
        "region_name":     region_name,
        "threat_level":    "safe",
        "probability_1h":  0.0,
        "probability_3h":  0.0,
        "probability_6h":  0.0,
        "probability_12h": 0.0,
        "updated_at":      now.isoformat() + "Z",
        "forecast":        {},
    }

    pred_data = _read_latest_json()
    if not pred_data:
        return res

    probs    = pred_data.get("regions_probabilities", {}).get(region_name, {})
    forecast = pred_data.get("regions_forecast",      {}).get(region_name, {})

    if probs:
        prob_values = list(probs.values())
        res["probability_1h"]  = round(float(prob_values[0]),            3) if len(prob_values) >= 1  else 0.0
        res["probability_3h"]  = round(sum(prob_values[:3])  / 3,        3) if len(prob_values) >= 3  else 0.0
        res["probability_6h"]  = round(sum(prob_values[:6])  / 6,        3) if len(prob_values) >= 6  else 0.0
        res["probability_12h"] = round(sum(prob_values[:12]) / 12,       3) if len(prob_values) >= 12 else 0.0
        res["threat_level"]    = _prob_to_threat(res["probability_1h"])
        res["updated_at"]      = pred_data.get("last_prediction_time", res["updated_at"])

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
            "alarm":       bool(forecast.get(hour_label, False)),
        })

    return {"region": region_slug, "hours": hours_out}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    parser.print_help()