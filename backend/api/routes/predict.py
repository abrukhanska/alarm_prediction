from fastapi import APIRouter, HTTPException
from ..data.mock_data import get_prediction
from ..models.schemas import PredictionResponse

router = APIRouter()

@router.get("/predict/{region}", response_model=PredictionResponse)
def predict(region: str):
    data = get_prediction(region.lower())
    if not data:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found")
    return data