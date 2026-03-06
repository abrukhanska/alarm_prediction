"use client";
import { WeatherResponse } from "@/lib/types";

interface WeatherCardProps {
  weather: WeatherResponse | null;
}

function Row({ icon, label, value }: { icon: string; label: string; value: string }) {
  return (
    <div className="flex items-center gap-2 text-sm py-1">
      <span className="text-base w-5">{icon}</span>
      <span style={{ color: "#64748b" }}>{label}</span>
      <span className="ml-auto font-medium" style={{ color: "#e2e8f0" }}>{value}</span>
    </div>
  );
}

export default function WeatherCard({ weather }: WeatherCardProps) {
  if (!weather) return null;

  const windDir = ["N","NE","E","SE","S","SW","W","NW"][
    Math.round(weather.winddir / 45) % 8
  ];

  return (
    <div className="rounded-lg p-3" style={{ background: "#0a0f1a", border: "1px solid #1e3a5f" }}>
      <p className="text-xs mb-2 font-semibold tracking-wider uppercase" style={{ color: "#64748b" }}>
        Weather
      </p>
      <div className="text-lg font-bold mb-1" style={{ color: "#06b6d4" }}>
        {weather.conditions}
      </div>
      <Row icon="🌡️" label="Temp" value={`${weather.temp}°C`} />
      <Row icon="💨" label="Wind" value={`${weather.windspeed} km/h ${windDir}`} />
      <Row icon="👁️" label="Visibility" value={`${weather.visibility} km`} />
      <Row icon="☁️" label="Cloud Cover" value={`${weather.cloudcover}%`} />
      <Row icon="💧" label="Humidity" value={`${weather.humidity}%`} />
      <Row icon="🌧️" label="Precip" value={`${weather.precip} mm`} />
    </div>
  );
}
