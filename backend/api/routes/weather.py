from fastapi import APIRouter, HTTPException
from ..data.mock_data import get_weather
from ..models.schemas import WeatherResponse

router = APIRouter()

@router.get("/weather/{region}", response_model=WeatherResponse)
def weather(region: str):
    data = get_weather(region.lower())
    if not data:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found")
    return data