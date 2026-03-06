from fastapi import APIRouter
from ..data.mock_data import get_current_alarms
from ..models.schemas import AlarmsResponse

router = APIRouter()

@router.get("/current-alarms", response_model=AlarmsResponse)
def current_alarms():
    return get_current_alarms()