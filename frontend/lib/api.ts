import type { ForecastResponse } from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8000";

export async function fetchForecast(): Promise<ForecastResponse> {
  const res = await fetch(`${API_BASE}/api/forecast`);
  if (!res.ok) throw new Error(`Forecast unavailable (${res.status})`);
  return res.json() as Promise<ForecastResponse>;
}

export async function triggerForecastUpdate(): Promise<{ status: string; message: string }> {
  const res = await fetch(`${API_BASE}/api/update-forecast`, { method: "POST" });
  if (!res.ok) throw new Error(`Update failed (${res.status})`);
  return res.json();
}

export async function triggerRetrain(): Promise<{ status: string; message: string }> {
  const res = await fetch(`${API_BASE}/api/admin/retrain`, { method: "POST" });
  if (!res.ok) throw new Error(`Retrain failed (${res.status})`);
  return res.json();
}

export async function fetchHealth(): Promise<Record<string, unknown>> {
  const res = await fetch(`${API_BASE}/api/health`);
  if (!res.ok) throw new Error("Health check failed");
  return res.json();
}