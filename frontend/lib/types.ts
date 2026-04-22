export type WeatherIcon =
  | "sun"
  | "partly-cloudy"
  | "cloud"
  | "drizzle"
  | "rain"
  | "snow";

export interface HourWeather {
  temp: string;
  wind: number;
  cloudcover: number;
  humidity: number;
  precip: number;
  icon: WeatherIcon;
}

export interface HourlyForecastItem {
  hour: string;
  probability: number;
  alarm: boolean;
  weather: HourWeather;
}

export interface RegionForecast {
  is_live_alarm_now: boolean;
  risk_level: string;
  max_probability: number;
  current_temp: number;
  hourly_data: HourlyForecastItem[];
}

export interface GlobalMetrics {
  national_risk_index: number;
  last_model_update: string;
  prediction_generated_at: string;
  base_datetime: string | null;
  forecast_start: string | null;
  forecast_end: string | null;
  forecast_hours: number;
  total_regions_at_risk: number;
  live_alarms_count: number;
  weather_live: boolean;
}

export interface ForecastResponse {
  global_metrics: GlobalMetrics;
  regions: Record<string, RegionForecast>;
}