"use client";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { TimelineHour } from "@/lib/types";

interface ForecastTimelineProps {
  hours: TimelineHour[];
  currentHour: number;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div
      className="rounded-lg px-3 py-2 text-xs"
      style={{ background: "#111827", border: "1px solid #1e3a5f" }}
    >
      <p className="font-bold mb-1" style={{ color: "#e2e8f0" }}>{label}</p>
      <p style={{ color: "#06b6d4" }}>
        Probability: {Math.round((payload[0]?.value ?? 0) * 100)}%
      </p>
      {payload[1] && (
        <p style={{ color: "#ef4444" }}>Missile: {Math.round(payload[1].value * 100)}%</p>
      )}
      {payload[2] && (
        <p style={{ color: "#f59e0b" }}>Drone: {Math.round(payload[2].value * 100)}%</p>
      )}
    </div>
  );
};

export default function ForecastTimeline({ hours, currentHour }: ForecastTimelineProps) {
  if (!hours.length) return null;

  const currentLabel = `${String(currentHour).padStart(2, "0")}:00`;

  return (
    <div className="w-full h-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={hours} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="probGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.4} />
              <stop offset="95%" stopColor="#06b6d4" stopOpacity={0.02} />
            </linearGradient>
            <linearGradient id="missileGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#ef4444" stopOpacity={0.0} />
            </linearGradient>
            <linearGradient id="droneGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#f59e0b" stopOpacity={0.0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e3a5f" />
          <XAxis
            dataKey="hour"
            stroke="#64748b"
            tick={{ fontSize: 10, fill: "#64748b" }}
            interval={2}
          />
          <YAxis
            stroke="#64748b"
            tick={{ fontSize: 10, fill: "#64748b" }}
            tickFormatter={(v) => `${Math.round(v * 100)}%`}
            domain={[0, 1]}
          />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine
            x={currentLabel}
            stroke="#06b6d4"
            strokeDasharray="4 4"
            label={{ value: "NOW", fill: "#06b6d4", fontSize: 10 }}
          />
          <Area
            type="monotone"
            dataKey="probability"
            stroke="#06b6d4"
            strokeWidth={2}
            fill="url(#probGrad)"
          />
          <Area
            type="monotone"
            dataKey="missile"
            stroke="#ef4444"
            strokeWidth={1}
            fill="url(#missileGrad)"
          />
          <Area
            type="monotone"
            dataKey="drone"
            stroke="#f59e0b"
            strokeWidth={1}
            fill="url(#droneGrad)"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
