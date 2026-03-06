export interface PredictionResponse {
  region: string;
  region_name: string;
  threat_level: string;
  probability_1h: number;
  probability_3h: number;
  probability_6h: number;
  probability_12h: number;
  threat_types: {
    missile: number;
    drone: number;
    artillery: number;
  };
  updated_at: string;
}

export interface RegionAlarm {
  id: string;
  name: string;
  active: boolean;
  type: string | null;
  since: string | null;
  threat_level: string;
}

export interface AlarmsResponse {
  timestamp: string;
  active_count: number;
  total_regions: number;
  regions: RegionAlarm[];
}

export interface WeatherResponse {
  region: string;
  temp: number;
  humidity: number;
  windspeed: number;
  winddir: number;
  visibility: number;
  cloudcover: number;
  pressure: number;
  conditions: string;
  precip: number;
}

export interface TimelineHour {
  hour: string;
  probability: number;
  missile: number;
  drone: number;
  artillery: number;
}

export interface TimelineResponse {
  region: string;
  hours: TimelineHour[];
}

export interface StatsResponse {
  active_alarms_count: number;
  total_regions: number;
  avg_threat_level: number;
  most_threatened_region: string;
  total_alarms_today: number;
  total_duration_today_hours: number;
}
