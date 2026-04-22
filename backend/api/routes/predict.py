import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from ..models.schemas import PredictionResponse
from ..data.real_data import REGION_SLUG_MAP

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LATEST_JSON  = PROJECT_ROOT / "data" / "predictions" / "latest.json"

THREAT_LEVELS = {
    range(0,  20):  "safe",
    range(20, 40):  "low",
    range(40, 60):  "medium",
    range(60, 80):  "high",
    range(80, 101): "critical",
}

def _prob_to_threat_local(prob: float) -> str:
    pct = int(prob * 100)
    for r, level in THREAT_LEVELS.items():
        if pct in r:
            return level
    return "medium"

def _read_latest() -> dict | None:
    if not LATEST_JSON.exists():
        return None
    try:
        with open(LATEST_JSON, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _build_prediction(region_key: str, data: dict) -> dict | None:
    regions_probabilities = data.get("regions_probabilities", {})
    regions_forecast      = data.get("regions_forecast",      {})

    matched_key = None
    if region_key in regions_probabilities:
        matched_key = region_key
    else:
        normalised = region_key.lower().replace(" ", "_").replace("-", "_")
        for k in regions_probabilities:
            if k.lower().replace(" ", "_").replace("-", "_") == normalised:
                matched_key = k
                break

    if matched_key is None:
        return None

    probs    = list(regions_probabilities[matched_key].values())   # floats
    forecast = regions_forecast.get(matched_key, {})

    if not probs:
        return None

    def _avg(vals: list[float], n: int) -> float:
        return round(sum(vals[:n]) / n, 3) if len(vals) >= n else round(sum(vals) / len(vals), 3)

    prob_1h  = round(float(probs[0]), 3)
    prob_3h  = _avg(probs, 3)
    prob_6h  = _avg(probs, 6)
    prob_12h = _avg(probs, 12)

    threat = _prob_to_threat_local(prob_1h).capitalize()

    return {
        "region":          matched_key,
        "region_name":     matched_key.replace("_", " ").title(),
        "threat_level":    threat,
        "probability_1h":  prob_1h,
        "probability_3h":  prob_3h,
        "probability_6h":  prob_6h,
        "probability_12h": prob_12h,
        "updated_at":      data.get(
            "last_prediction_time",
            datetime.now(timezone.utc).isoformat(),
        ),
        "forecast": forecast,
    }

@router.get("/predict/all")
def predict_all():
    data = _read_latest()
    if not data:
        raise HTTPException(
            status_code=503,
            detail="Predictions unavailable. POST /api/update-forecast first.",
        )

    results = {}
    for region_key in data.get("regions_probabilities", {}):
        pred = _build_prediction(region_key, data)
        if pred:
            results[region_key] = pred

    if not results:
        raise HTTPException(status_code=404, detail="No region predictions found in latest.json")

    return {
        "regions":              results,
        "last_prediction_time": data.get("last_prediction_time"),
        "model_name":           data.get("model_name"),
        "threshold":            data.get("threshold"),
    }

@router.get("/predict/{region}", response_model=PredictionResponse)
def predict_region(region: str):
    if region.lower() == "all":
        raise HTTPException(status_code=400, detail="Use /predict/all for all regions")

    data = _read_latest()
    if not data:
        raise HTTPException(
            status_code=503,
            detail="Predictions unavailable. POST /api/update-forecast first.",
        )

    pred = _build_prediction(region, data)
    if not pred:
        available = sorted(data.get("regions_probabilities", {}).keys())
        raise HTTPException(
            status_code=404,
            detail=f"Region '{region}' not found. Available: {available}",
        )
    return pred