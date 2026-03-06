from fastapi import APIRouter, HTTPException
from ..data.mock_data import get_timeline
from ..models.schemas import TimelineResponse

router = APIRouter()

@router.get("/timeline/{region}", response_model=TimelineResponse)
def timeline(region: str):
    data = get_timeline(region.lower())
    if not data:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found")
    return data