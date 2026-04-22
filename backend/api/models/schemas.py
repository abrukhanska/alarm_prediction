from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field

class PredictionResponse(BaseModel):
    region:      str
    region_name: str
    threat_level: str

    probability_1h:  float = Field(ge=0.0, le=1.0)
    probability_3h:  float = Field(ge=0.0, le=1.0)
    probability_6h:  float = Field(ge=0.0, le=1.0)
    probability_12h: float = Field(ge=0.0, le=1.0)

    updated_at:   str
    forecast:     Optional[dict[str, bool]] = None

class RegionAlarm(BaseModel):
    id:           str
    name:         str
    active:       bool
    type:         Optional[str] = None
    since:        Optional[str] = None
    threat_level: str
    probability:  float = 0.0

class AlarmsResponse(BaseModel):
    timestamp:     str
    active_count:  int
    total_regions: int
    regions:       list[RegionAlarm]

class WeatherResponse(BaseModel):
    region:     str
    temp:       float
    humidity:   float
    windspeed:  float
    winddir:    float
    visibility: float
    cloudcover: float
    pressure:   float
    conditions: str
    precip:     float

class TimelineHour(BaseModel):
    hour:        str
    probability: float = Field(ge=0.0, le=1.0)
    alarm:       bool  = False

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

class HourWeather(BaseModel):
    temp:       str
    wind:       int
    cloudcover: int
    humidity:   int
    precip:     float
    icon:       str

class HourlyForecastItem(BaseModel):
    hour:        str
    probability: int
    alarm:       bool
    weather:     HourWeather

class RegionForecast(BaseModel):
    is_live_alarm_now: bool
    risk_level:        str
    max_probability:   float
    hourly_data:       list[HourlyForecastItem]
    current_temp: Optional[float] = 0.0

class GlobalMetrics(BaseModel):
    national_risk_index:   int
    last_model_update:       str
    prediction_generated_at: str

    base_datetime:  Optional[str] = None
    forecast_start: Optional[str] = None
    forecast_end:   Optional[str] = None
    forecast_hours: int = 24

    total_regions_at_risk: int
    live_alarms_count:     int
    weather_live: bool = False  # True when weather_today.json is available

class GodForecastResponse(BaseModel):
    global_metrics: GlobalMetrics
    regions:        dict[str, RegionForecast]