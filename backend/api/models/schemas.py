from pydantic import BaseModel, Field
from typing import Optional

class ThreatTypes(BaseModel):
    missile:   float = 0.0
    drone:     float = 0.0
    artillery: float = 0.0


class PredictionResponse(BaseModel):
    region:      str
    region_name: str
    threat_level: str

    probability_1h:  float = Field(ge=0.0, le=1.0)
    probability_3h:  float = Field(ge=0.0, le=1.0)
    probability_6h:  float = Field(ge=0.0, le=1.0)
    probability_12h: float = Field(ge=0.0, le=1.0)

    threat_types: ThreatTypes

    updated_at: str

    forecast: Optional[dict[str, bool]] = None

class RegionAlarm(BaseModel):
    id:    str
    name:  str
    active: bool
    type:  Optional[str] = None
    since: Optional[str] = None
    threat_level: str
    probability: float = 0.0


class AlarmsResponse(BaseModel):
    timestamp:     str
    active_count:  int
    total_regions: int
    regions: list[RegionAlarm]

class WeatherResponse(BaseModel):
    region:      str
    temp:        float
    humidity:    float
    windspeed:   float
    winddir:     float
    visibility:  float
    cloudcover:  float
    pressure:    float
    conditions:  str
    precip:      float

class TimelineHour(BaseModel):
    hour:        str
    probability: float = Field(ge=0.0, le=1.0)
    missile:     float = Field(default=0.0, ge=0.0, le=1.0)
    drone:       float = Field(default=0.0, ge=0.0, le=1.0)
    artillery:   float = Field(default=0.0, ge=0.0, le=1.0)

class TimelineResponse(BaseModel):
    region: str
    hours:  list[TimelineHour]

class StatsResponse(BaseModel):
    active_alarms_count:        int
    total_regions:              int
    avg_threat_level:           float
    most_threatened_region:     str
    total_alarms_today:         int
    total_duration_today_hours: float