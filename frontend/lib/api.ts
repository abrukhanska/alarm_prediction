const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8000";

export async function fetchPrediction(region: string) {
  const res = await fetch(`${API_BASE}/api/predict/${region}`);
  if (!res.ok) throw new Error(`Failed to fetch prediction for ${region}`);
  return res.json();
}

export async function fetchCurrentAlarms() {
  const res = await fetch(`${API_BASE}/api/current-alarms`);
  if (!res.ok) throw new Error("Failed to fetch current alarms");
  return res.json();
}

export async function fetchWeather(region: string) {
  const res = await fetch(`${API_BASE}/api/weather/${region}`);
  if (!res.ok) throw new Error(`Failed to fetch weather for ${region}`);
  return res.json();
}

export async function fetchTimeline(region: string) {
  const res = await fetch(`${API_BASE}/api/timeline/${region}`);
  if (!res.ok) throw new Error(`Failed to fetch timeline for ${region}`);
  return res.json();
}

export async function fetchStats() {
  const res = await fetch(`${API_BASE}/api/stats`);
  if (!res.ok) throw new Error("Failed to fetch stats");
  return res.json();
}

export async function fetchForecast(region: string = "all") {
  const res = await fetch(`${API_BASE}/api/forecast?region=${encodeURIComponent(region)}`);
  if (!res.ok) throw new Error("Failed to fetch forecast");
  return res.json();
}

export async function triggerForecastUpdate() {
  const res = await fetch(`${API_BASE}/api/update-forecast`, { method: "POST" });
  if (!res.ok) throw new Error("Failed to trigger forecast update");
  return res.json();
}