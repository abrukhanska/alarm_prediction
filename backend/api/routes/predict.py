import json
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from ..models.schemas import PredictionResponse

router = APIRouter()

PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent
MODELS_DIR     = PROJECT_ROOT / "models"
MODEL_PKL      = MODELS_DIR / "lightgbm_model.pkl"
THR_JSON       = MODELS_DIR / "lightgbm_threshold.json"
LATEST_JSON    = PROJECT_ROOT / "data" / "predictions" / "latest.json"


_model_cache = {"model": None, "mtime": 0.0, "threshold": 0.5}


def _get_model():
    
    if not MODEL_PKL.exists():
        return None, 0.5

    current_mtime = os.path.getmtime(MODEL_PKL)
    if _model_cache["model"] is None or current_mtime > _model_cache["mtime"]:
        with open(MODEL_PKL, "rb") as f:
            _model_cache["model"] = pickle.load(f)
        _model_cache["mtime"] = current_mtime

        if THR_JSON.exists():
            with open(THR_JSON) as f:
                _model_cache["threshold"] = json.load(f).get("threshold", 0.5)

        print(f"[predict_service] Model reloaded at mtime={current_mtime:.0f}")

    return _model_cache["model"], _model_cache["threshold"]


def _read_latest_for_region(region_key: str) -> dict | None:
    
    if not LATEST_JSON.exists():
        return None

    with open(LATEST_JSON, encoding="utf-8") as f:
        data = json.load(f)

    regions = data.get("regions_forecast", {})

    
    if region_key in regions:
        forecast = regions[region_key]
    else:
        match = next(
            (k for k in regions if k.lower().replace(" ", "_") == region_key.lower().replace(" ", "_")),
            None
        )
        if match is None:
            return None
        forecast = regions[match]
        region_key = match

    
    hours = list(forecast.values())
    total = len(hours) if hours else 1
    alarm_count = sum(1 for v in hours if v)

    prob_1h  = float(forecast.get("01:00", False))
    prob_3h  = sum(1 for h, v in forecast.items() if int(h[:2]) <= 3 and v) / 3
    prob_6h  = sum(1 for h, v in forecast.items() if int(h[:2]) <= 6 and v) / 6
    prob_12h = sum(1 for h, v in forecast.items() if int(h[:2]) <= 12 and v) / 12

    overall = alarm_count / total
    if overall >= 0.6:
        threat_level = "Critical"
    elif overall >= 0.35:
        threat_level = "High"
    elif overall >= 0.15:
        threat_level = "Medium"
    else:
        threat_level = "Low"

    return {
        "region":         region_key,
        "region_name":    region_key.replace("_", " ").title(),
        "threat_level":   threat_level,
        "probability_1h":  round(prob_1h,  2),
        "probability_3h":  round(prob_3h,  2),
        "probability_6h":  round(prob_6h,  2),
        "probability_12h": round(prob_12h, 2),
        "threat_types":   {"missile": 0.4, "drone": 0.5, "artillery": 0.1},
        "updated_at":     data.get("last_prediction_time", datetime.now(timezone.utc).isoformat()),
    }


@router.get("/predict/{region}", response_model=PredictionResponse)
def predict(region: str):
    data = _read_latest_for_region(region.lower())
    if not data:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found or predictions unavailable")
    return data