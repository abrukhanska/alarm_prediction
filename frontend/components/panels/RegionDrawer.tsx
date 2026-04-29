"use client";
import { useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from "recharts";
import type { RegionForecast } from "@/lib/types";
import { probToColor, hexToRgba, RISK_LEVEL_LABELS } from "@/lib/colors";
import WeatherIcon from "@/components/ui/WeatherIcon";

interface RegionDrawerProps {
  regionName: string | null;
  regionData: RegionForecast | null;
  selectedHour: number;
  onClose: () => void;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  const prob = payload.find((p: any) => p.dataKey === "probability")?.value ?? 0;
  const wind = payload.find((p: any) => p.dataKey === "wind")?.value ?? 0;
  const color = probToColor(prob);
  return (
    <div
      className="rounded-lg px-3 py-2 text-xs"
      style={{
        background: "rgba(5,10,22,0.97)",
        border: `1px solid ${hexToRgba(color, 0.4)}`,
        fontFamily: "'Share Tech Mono', monospace",
      }}
    >
      <div className="font-bold mb-1" style={{ color: "#e2e8f0" }}>{label}</div>
      <div style={{ color }}>{prob}% THREAT</div>
      <div style={{ color: "#60a5fa" }}>💨 {wind} km/h wind</div>
    </div>
  );
};

export default function RegionDrawer({
  regionName,
  regionData,
  selectedHour,
  onClose,
}: RegionDrawerProps) {
  const isOpen = !!regionName && !!regionData;

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  if (!isOpen || !regionData) return null;

  const displayName = regionName!
    .replace(" Oblast", "")
    .replace("City of ", "")
    .toUpperCase();

  const currentHourData = regionData.hourly_data[selectedHour];
  const currentProb = currentHourData?.probability ?? 0;
  const currentWeather = currentHourData?.weather;
  const isLive = regionData.is_live_alarm_now;
  const color = probToColor(currentProb);

  const chartData = regionData.hourly_data.map((item) => ({
    hour: item.hour.includes("T") ? item.hour.slice(11, 16) : item.hour,
    probability: item.probability,
    wind: item.weather.wind,
    alarm: item.alarm,
    cloudcover: item.weather.cloudcover,
    humidity: item.weather.humidity,
  }));

  const alarmHours = regionData.hourly_data.filter((h) => h.alarm);

  return (
    <AnimatePresence>
      <motion.div
        key="overlay"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="fixed inset-0 z-40"
        style={{ background: "rgba(0,0,0,0.4)" }}
        onClick={onClose}
      />

      <motion.div
        key="drawer"
        initial={{ x: "100%" }}
        animate={{ x: 0 }}
        exit={{ x: "100%" }}
        transition={{ type: "spring", damping: 28, stiffness: 220 }}
        className="fixed right-0 top-0 bottom-0 z-50 overflow-y-auto"
        style={{
          width: 380,
          maxWidth: "94vw",
          background: "rgba(4, 8, 18, 0.97)",
          borderLeft: `1px solid ${hexToRgba(color, 0.3)}`,
          boxShadow: `-10px 0 40px ${hexToRgba(color, 0.08)}`,
          fontFamily: "'Share Tech Mono', monospace",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="h-0.5 w-full" style={{ background: `linear-gradient(90deg, ${color}, transparent)` }} />

        <div className="p-5 flex flex-col gap-5">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 mb-0.5">
                {isLive && (
                  <span
                    className="inline-block w-2 h-2 rounded-full animate-pulse"
                    style={{ background: "#ff1a3d", boxShadow: "0 0 6px #ff1a3d" }}
                  />
                )}
                <h2
                  className="text-xl font-black tracking-[0.08em]"
                  style={{ color: "#e2e8f0", letterSpacing: "0.06em" }}
                >
                  {displayName}
                </h2>
              </div>
              <div className="text-[9px] tracking-[0.2em] uppercase" style={{ color: "#475569" }}>
                {isLive ? "⚡ LIVE ALARM IN EFFECT" : "FORECAST ANALYSIS"} · {RISK_LEVEL_LABELS[regionData.risk_level] ?? regionData.risk_level}
              </div>
            </div>
            <button
              onClick={onClose}
              className="w-7 h-7 flex items-center justify-center rounded-full border text-lg transition-colors duration-150"
              style={{
                borderColor: "rgba(255,255,255,0.1)",
                color: "#64748b",
              }}
            >
              ×
            </button>
          </div>

          <div className="grid grid-cols-3 gap-2">
            <div
              className="rounded-lg p-3 flex flex-col items-center"
              style={{
                background: hexToRgba(color, 0.08),
                border: `1px solid ${hexToRgba(color, 0.25)}`,
              }}
            >
              <span className="text-2xl font-black" style={{ color }}>
                {currentProb}
                <span className="text-sm">%</span>
              </span>
              <span className="text-[8px] tracking-[0.15em] uppercase mt-0.5" style={{ color: "#64748b" }}>
                THREAT
              </span>
            </div>

            <div
              className="rounded-lg p-3 flex flex-col items-center"
              style={{ background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.07)" }}
            >
              <span className="text-2xl font-black" style={{ color: probToColor(regionData.max_probability) }}>
                {Math.round(regionData.max_probability * 100)}
                <span className="text-sm">%</span>
              </span>
              <span className="text-[8px] tracking-[0.15em] uppercase mt-0.5" style={{ color: "#64748b" }}>
                24H MAX
              </span>
            </div>

            <div
              className="rounded-lg p-3 flex flex-col items-center gap-1"
              style={{ background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.07)" }}
            >
              {currentWeather && (
                <>
                  <div className="flex items-center gap-1">
                    <WeatherIcon icon={currentWeather.icon} size={16} />
                    <span className="text-base font-black" style={{ color: "#94a3b8" }}>
                      {currentWeather.temp}°
                    </span>
                  </div>
                  <span className="text-[8px] tracking-[0.15em] uppercase" style={{ color: "#64748b" }}>
                    WEATHER
                  </span>
                </>
              )}
            </div>
          </div>

          <div>
            <div className="text-[9px] font-black tracking-[0.2em] uppercase mb-2 flex items-center gap-2"
              style={{ color: "#4fc3f7" }}>
              <div className="w-1 h-3 rounded-full" style={{ background: "#4fc3f7" }} />
              CORRELATION: THREAT × WEATHER
            </div>

            <div className="text-[8px] mb-2 flex gap-4" style={{ color: "#64748b" }}>
              <span className="flex items-center gap-1">
                <div className="w-3 h-0.5 rounded" style={{ background: "#ef4444" }} />
                Threat probability
              </span>
              <span className="flex items-center gap-1">
                <div className="w-3 h-2 rounded-sm opacity-50" style={{ background: "#3b82f6" }} />
                Wind speed (km/h)
              </span>
            </div>

            <div style={{ width: "100%", height: 160 }}>
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData} margin={{ top: 4, right: 8, left: -14, bottom: 0 }}>
                <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.04)" />
                <XAxis
                  dataKey="hour"
                  tick={{ fontSize: 8, fill: "#475569", fontFamily: "'Share Tech Mono', monospace" }}
                  tickLine={false}
                  axisLine={false}
                  interval={3}
                />
                <YAxis
                  yAxisId="prob"
                  domain={[0, 100]}
                  tick={{ fontSize: 8, fill: "#ef4444", fontFamily: "'Share Tech Mono', monospace" }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => `${v}%`}
                  width={28}
                />
                <YAxis
                  yAxisId="wind"
                  orientation="right"
                  domain={[0, "auto"]}
                  tick={{ fontSize: 8, fill: "#3b82f6", fontFamily: "'Share Tech Mono', monospace" }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => `${v}`}
                  width={22}
                />
                <Tooltip content={<CustomTooltip />} />
                <ReferenceLine
                  yAxisId="prob"
                  x={chartData[selectedHour]?.hour}
                  stroke="rgba(0,229,255,0.5)"
                  strokeDasharray="3 3"
                  strokeWidth={1}
                />

                <Bar yAxisId="wind" dataKey="wind" opacity={0.35} radius={[1, 1, 0, 0]}>
                  {chartData.map((entry, i) => (
                    <Cell key={i} fill={i === selectedHour ? "#60a5fa" : "#1d4ed8"} />
                  ))}
                </Bar>

                <Line
                  yAxisId="prob"
                  type="monotone"
                  dataKey="probability"
                  stroke="#ef4444"
                  strokeWidth={2}
                  dot={(props) => {
                    const { cx, cy, index, payload } = props;
                    if (!payload.alarm && index !== selectedHour) return <g key={index} />;
                    return (
                      <circle
                        key={index}
                        cx={cx}
                        cy={cy}
                        r={index === selectedHour ? 4 : 3}
                        fill={index === selectedHour ? "#00e5ff" : "#ff1a3d"}
                        stroke="none"
                      />
                    );
                  }}
                  activeDot={{ r: 5, fill: "#ef4444", stroke: "#ff1a3d" }}
                />
              </ComposedChart>
            </ResponsiveContainer>
            </div>

            <div
              className="text-[8px] text-center opacity-40 mt-1"
              style={{ color: "#94a3b8", fontFamily: "sans-serif" }}
            >
              UAV operations peak during low-wind, low-visibility windows → weather–threat correlation
            </div>
          </div>

          <div>
            <div className="text-[9px] font-black tracking-[0.2em] uppercase mb-2" style={{ color: "#4fc3f7" }}>
              PREDICTED ALARM WINDOWS
            </div>
            {alarmHours.length > 0 ? (
              <div className="flex flex-wrap gap-1.5">
                {alarmHours.map((h) => (
                  <div
                    key={h.hour}
                    className="px-2 py-0.5 rounded text-[9px] font-bold flex items-center gap-1"
                    style={{
                      background: "rgba(255,26,61,0.1)",
                      border: "1px solid rgba(255,26,61,0.3)",
                      color: "#ff6680",
                    }}
                  >
                   <WeatherIcon icon={h.weather.icon} size={12} />
                    {h.hour.includes("T") ? h.hour.slice(11, 16) : h.hour}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-[10px]" style={{ color: "#334155" }}>
                No alarm windows predicted in this forecast period.
              </div>
            )}
          </div>

          {currentWeather && (
            <div>
              <div className="text-[9px] font-black tracking-[0.2em] uppercase mb-2" style={{ color: "#4fc3f7" }}>
                WEATHER AT {currentHourData?.hour?.includes("T")
                  ? currentHourData.hour.slice(11, 16)
                  : currentHourData?.hour}
              </div>
              <div
                className="rounded-lg p-3 grid grid-cols-3 gap-2"
                style={{ background: "rgba(10,20,40,0.6)", border: "1px solid rgba(255,255,255,0.06)" }}
              >
                {[
                  { icon: "💨", label: "WIND", value: `${currentWeather.wind} km/h` },
                  { icon: "☁", label: "CLOUD", value: `${currentWeather.cloudcover}%` },
                  { icon: "💧", label: "HUMIDITY", value: `${currentWeather.humidity}%` },
                  { icon: "🌧", label: "PRECIP", value: `${currentWeather.precip} mm` },
                  { icon: "🌡", label: "TEMP", value: `${currentWeather.temp}°C` },
                  {
                    icon: "📡",
                    label: "RISK",
                    value: RISK_LEVEL_LABELS[regionData.risk_level] ?? regionData.risk_level,
                  },
                ].map(({ icon, label, value }) => (
                  <div key={label} className="flex flex-col items-center py-1">
                    <span className="text-base mb-0.5">{icon}</span>
                    <span className="text-[8px] tracking-wider uppercase" style={{ color: "#475569" }}>
                      {label}
                    </span>
                    <span className="text-[11px] font-bold mt-0.5" style={{ color: "#94a3b8" }}>
                      {value}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </motion.div>
    </AnimatePresence>
  );
}