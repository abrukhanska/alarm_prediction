import json
import subprocess
import sys
import time
import fcntl
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

from ..data.real_data import (
    get_weather,
    REGION_NAME_TO_SLUG,
    _live_state,
    _weather_today,
)

from ..models.schemas import GodForecastResponse

router = APIRouter()

PROJECT_ROOT    = Path(__file__).resolve().parent.parent.parent.parent
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
LATEST_JSON     = PREDICTIONS_DIR / "latest.json"
PREDICT_SCRIPT  = PROJECT_ROOT / "models" / "predict_24h.py"
RETRAIN_SCRIPT  = PROJECT_ROOT / "models" / "retrain.py"
VENV_PYTHON     = PROJECT_ROOT / "venv" / "bin" / "python"

def _python_executable() -> str:
    return str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

def _hhmm(hour_label: str) -> str:
    if "T" in hour_label:
        return hour_label[11:16]
    return hour_label

def _icon(temp: float, precip: float, cloudcover: float) -> str:
    if temp < 0 and precip > 0.5: return "snow"
    if precip > 1.0:               return "rain"
    if precip > 0.2:               return "drizzle"
    if cloudcover > 70:            return "cloud"
    if cloudcover > 30:            return "partly-cloudy"
    return "sun"

def _to_hour_weather(w: dict) -> dict:
    temp_val   = float(w.get("temp",       0) or 0)
    precip     = float(w.get("precip",     0) or 0)
    cloudcover = float(w.get("cloudcover", 0) or 0)
    return {
        "temp":       f"+{temp_val:.0f}" if temp_val >= 0 else f"{temp_val:.0f}",
        "wind":       int(round(float(w.get("windspeed", 0) or 0))),
        "cloudcover": int(round(cloudcover)),
        "humidity":   int(round(float(w.get("humidity",  0) or 0))),
        "precip":     round(precip, 1),
        "icon":       _icon(temp_val, precip, cloudcover),
    }

def _is_ml_busy() -> bool:
    lock_file = "/tmp/ml_heavy.lock"
    try:
        fd = os.open(lock_file, os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        return False
    except BlockingIOError:
        return True

def _run_predict() -> None:
    cmd = f"flock -n /tmp/ml_heavy.lock {_python_executable()} {PREDICT_SCRIPT}"
    try:
        subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"[{PREDICT_SCRIPT.name}] Background predict launched safely.")
    except Exception as e:
        print(f"[{PREDICT_SCRIPT.name}] Exception during launch: {e}")

def _run_retrain() -> None:
    cmd = f"flock -n /tmp/ml_heavy.lock {_python_executable()} {RETRAIN_SCRIPT} --validation-days 7"
    try:
        subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"[{RETRAIN_SCRIPT.name}] Background retrain launched safely.")
    except Exception as e:
        print(f"[{RETRAIN_SCRIPT.name}] Exception during launch: {e}")

@router.get("/api/forecast", response_model=GodForecastResponse)
async def get_god_forecast():
    if not LATEST_JSON.exists():
        raise HTTPException(
            status_code=503,
            detail=(
                "Predictions not yet available. "
                "POST /api/update-forecast and wait ~30s."
            ),
        )

    try:
        with open(LATEST_JSON, encoding="utf-8") as f:
            pred_data = json.load(f)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Forecast data is being regenerated, try again shortly. ({e})",
        )

    live = _live_state()
    live_region_active: dict[str, bool] = {}
    live_alarms_count: int = 0
    if live:
        live_region_active = {
            name: bool(is_active)
            for name, is_active in live.get("regions", {}).items()
        }
        live_alarms_count = int(live.get("active_alarms_count", 0))

    patched_regions: dict = {}

    for region_name, region_data in pred_data.get("regions", {}).items():
        slug: str | None = REGION_NAME_TO_SLUG.get(region_name)
        if slug:
            live_w       = get_weather(slug)
            current_temp = float(live_w.get("temp", 0) or 0)
        else:
            current_temp = float(region_data.get("current_temp", 0.0))

        patched_hourly = []
        for item in region_data.get("hourly_data", []):
            hour_label  = item["hour"]
            hhmm        = _hhmm(hour_label)
            raw_w        = get_weather(slug, hour=hhmm) if slug else {}
            weather_dict = _to_hour_weather(raw_w)

            patched_hourly.append({
                "hour":        hour_label,
                "probability": int(item["probability"]),
                "alarm":       bool(item["alarm"]),
                "weather":     weather_dict,
            })

        patched_regions[region_name] = {
            "is_live_alarm_now": live_region_active.get(region_name, False),
            "risk_level":        region_data.get("risk_level", "GREEN"),
            "max_probability":   float(region_data.get("max_probability", 0.0)),
            "current_temp":      current_temp,
            "hourly_data":       patched_hourly,
        }

    gm        = pred_data.get("global_metrics", {})
    wtoday_ok = _weather_today() is not None

    global_metrics = {
        "national_risk_index":     int(gm.get("national_risk_index", 0)),
        "last_model_update":       gm.get(
            "last_model_update",
            pred_data.get("last_model_update", "unknown"),
        ),
        "prediction_generated_at": gm.get(
            "prediction_generated_at",
            pred_data.get("last_prediction_time", "unknown"),
        ),
        "base_datetime":  gm.get("base_datetime",  pred_data.get("base_datetime",  None)),
        "forecast_start": gm.get("forecast_start", pred_data.get("forecast_start", None)),
        "forecast_end":   gm.get("forecast_end",   pred_data.get("forecast_end",   None)),
        "forecast_hours": int(gm.get("forecast_hours", 24)),
        "total_regions_at_risk": int(gm.get("total_regions_at_risk", 0)),
        "live_alarms_count":     live_alarms_count,
        "weather_live":          wtoday_ok,
    }

    return {
        "global_metrics": global_metrics,
        "regions":        patched_regions,
    }

@router.post("/api/update-forecast")
async def update_forecast(background_tasks: BackgroundTasks):
    if _is_ml_busy():
        raise HTTPException(
            status_code=429,
            detail="Сервер зараз виконує важкий ML-процес (оновлення даних або ретрейн). Будь ласка, зачекайте кілька хвилин.",
        )

    background_tasks.add_task(_run_predict)
    return {
        "status": "accepted",
        "message": "Prediction update started in background. Server locked.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "check_at": "GET /api/forecast (available in ~30s)",
    }

@router.post("/api/admin/retrain")
async def trigger_retrain(background_tasks: BackgroundTasks):
    if not RETRAIN_SCRIPT.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Retrain script not found: {RETRAIN_SCRIPT}",
        )
    if _is_ml_busy():
        raise HTTPException(
            status_code=429,
            detail="Сервер зараз виконує обробку даних або інший ретрейн. Будь ласка, зачекайте 15-30 хвилин.",
        )

    background_tasks.add_task(_run_retrain)
    return {
        "status": "CI/CD Pipeline started",
        "message": "A/B validation and model retrain launched in background. Server is locked for other ML tasks.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "script": str(RETRAIN_SCRIPT),
    }

@router.get("/api/health")
async def health():
    live = _live_state()
    wtoday = _weather_today()

    is_busy = _is_ml_busy()

    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ml_server_busy": is_busy,
        "latest_json_exists": LATEST_JSON.exists(),
        "latest_json_age_s": (
            round(
                datetime.now(timezone.utc).timestamp() - LATEST_JSON.stat().st_mtime,
                1,
            )
            if LATEST_JSON.exists() else None
        ),
        "live_state_ok": live is not None,
        "live_alarms_count": int(live.get("active_alarms_count", 0)) if live else None,
        "weather_today_ok": wtoday is not None,
        "weather_today_generated_at": wtoday.get("generated_at") if wtoday else None,
    }