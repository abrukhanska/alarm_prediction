from fastapi import APIRouter
from ..data.mock_data import get_stats
from ..models.schemas import StatsResponse

router = APIRouter()

@router.get("/stats", response_model=StatsResponse)
def stats():
    return get_stats()