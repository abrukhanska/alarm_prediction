import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from ..models.schemas import PredictionResponse

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LATEST_JSON  = PROJECT_ROOT / "data" / "predictions" / "latest.json"

def _read_latest_for_region(region_key: str) -> dict | None:

    if not LATEST_JSON.exists():
        return None

    with open(LATEST_JSON, encoding="utf-8") as f:
        data = json.load(f)

    if region_key.lower() == "all":
        return data

    regions = data.get("regions_forecast", {})

    if region_key in regions:
        forecast = regions[region_key]
    else:
        match = next(
            (
                k for k in regions
                if k.lower().replace(" ", "_") == region_key.lower().replace(" ", "_")
            ),
            None,
        )
        if match is None:
            return None
        forecast = regions[match]
        region_key = match

    booleans    = list(forecast.values())
    total       = len(booleans) or 1
    alarm_count = sum(1 for v in booleans if v)

    prob_1h  = float(booleans[0])                              if len(booleans) >= 1  else 0.0
    prob_3h  = sum(booleans[:3])  / min(len(booleans[:3]),  3) if len(booleans) >= 3  else 0.0
    prob_6h  = sum(booleans[:6])  / min(len(booleans[:6]),  6) if len(booleans) >= 6  else 0.0
    prob_12h = sum(booleans[:12]) / min(len(booleans[:12]),12) if len(booleans) >= 12 else 0.0

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
        "region":      region_key,
        "region_name": region_key.replace("_", " ").title(),
        "threat_level": threat_level,

        "probability_1h":  round(prob_1h,  2),
        "probability_3h":  round(prob_3h,  2),
        "probability_6h":  round(prob_6h,  2),
        "probability_12h": round(prob_12h, 2),

        "threat_types": {"missile": 0.0, "drone": 0.0, "artillery": 0.0},

        "updated_at": data.get(
            "last_prediction_time",
            datetime.now(timezone.utc).isoformat(),
        ),
        "forecast": forecast,
    }

@router.get("/predict/all")
def predict_all():
    data = _read_latest_for_region("all")
    if not data:
        raise HTTPException(
            status_code=404,
            detail="Predictions for all regions are currently unavailable",
        )
    return data

@router.get("/predict/{region}", response_model=PredictionResponse)
def predict_region(region: str):
    if region.lower() == "all":
        raise HTTPException(
            status_code=400,
            detail="Use /predict/all endpoint for all regions",
        )

    data = _read_latest_for_region(region)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"Region '{region}' not found or predictions unavailable",
        )
    return data