import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

router = APIRouter()

PROJECT_ROOT    = Path(__file__).resolve().parent.parent.parent
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
LATEST_JSON     = PREDICTIONS_DIR / "latest.json"
PREDICT_SCRIPT  = PROJECT_ROOT / "ml" / "predict_24h.py"


def _run_predict():
    
    try:
        result = subprocess.run(
            [sys.executable, str(PREDICT_SCRIPT)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            print(f"[predict] ERROR: {result.stderr[-500:]}")
        else:
            print(f"[predict] OK: {result.stdout[-200:]}")
    except subprocess.TimeoutExpired:
        print("[predict] TIMEOUT: predict_24h.py не завершився за 120s")
    except Exception as e:
        print(f"[predict] Exception: {e}")


@router.post("/api/update-forecast")
async def update_forecast(background_tasks: BackgroundTasks):
    
    background_tasks.add_task(_run_predict)
    return {
        "status":    "accepted",
        "message":   "Prediction update started in background",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "check_at":  "/api/forecast (доступно через ~30s)",
    }


@router.get("/api/forecast")
async def get_forecast(region: str = "all"):
    
    if not LATEST_JSON.exists():
        _run_predict()

    if not LATEST_JSON.exists():
        raise HTTPException(
            status_code=503,
            detail="Predictions not available yet. Run POST /api/update-forecast first."
        )

    with open(LATEST_JSON, encoding="utf-8") as f:
        data = json.load(f)

    response = {
        "last_model_train_time": data.get("last_model_train_time", "unknown"),
        "last_prediction_time":  data.get("last_prediction_time",  "unknown"),
        "model_name":    data.get("model_name",    "LightGBM"),
        "model_version": data.get("model_version", "v2"),
        "team_id":       data.get("team_id",       "aegis"),
        "threshold":     data.get("threshold",     0.5),
    }

    if region == "all":
        response["regions_forecast"] = data.get("regions_forecast", {})
    else:
        regions = data.get("regions_forecast", {})
        if region not in regions:
            available = sorted(regions.keys())
            raise HTTPException(
                status_code=404,
                detail=f"Region '{region}' not found. Available: {available}"
            )
        response["regions_forecast"] = {region: regions[region]}

    return JSONResponse(content=response)


@router.get("/api/health")
async def health():
    return {
        "status":              "ok",
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "latest_json_exists":  LATEST_JSON.exists(),
    }


@router.post("/admin/reload-model")
async def reload_model():
    
    try:
        from backend.predict_service import _model_cache
        _model_cache["model"] = None
        _model_cache["mtime"] = 0.0
        return {"status": "ok", "message": "Model cache cleared."}
    except ImportError:
        return {"status": "ok", "message": "predict_service not found, skipped."}