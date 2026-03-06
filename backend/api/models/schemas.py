from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class PredictionResponse(BaseModel):
    region: str
    region_name: str
    threat_level: str
    probability_1h: float
    probability_3h: float
    probability_6h: float
    probability_12h: float
    threat_types: dict
    updated_at: str

class RegionAlarm(BaseModel):
    id: str
    name: str
    active: bool
    type: Optional[str]
    since: Optional[str]
    threat_level: str

class AlarmsResponse(BaseModel):
    timestamp: str
    active_count: int
    total_regions: int
    regions: list[RegionAlarm]

class WeatherResponse(BaseModel):
    region: str
    temp: float
    humidity: float
    windspeed: float
    winddir: float
    visibility: float
    cloudcover: float
    pressure: float
    conditions: str
    precip: float

class TimelineHour(BaseModel):
    hour: str
    probability: float
    missile: float
    drone: float
    artillery: float

class TimelineResponse(BaseModel):
    region: str
    hours: list[TimelineHour]

class StatsResponse(BaseModel):
    active_alarms_count: int
    total_regions: int
    avg_threat_level: float
    most_threatened_region: str
    total_alarms_today: int
    total_duration_today_hours: float