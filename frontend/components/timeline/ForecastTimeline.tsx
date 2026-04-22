"use client";
import { useEffect, useRef, useCallback } from "react";
import { motion } from "framer-motion";
import type { ForecastResponse, RegionForecast } from "@/lib/types";
import { probToColor, hexToRgba } from "@/lib/colors";
import WeatherIcon from "@/components/ui/WeatherIcon";

interface ForecastTimelineProps {
  forecast: ForecastResponse;
  selectedHour: number;
  liveHour: number;
  onHourChange: (h: number | ((prev: number) => number)) => void;
  isPlaying: boolean;
  onTogglePlay: () => void;
  selectedRegion?: string | null;
}

function nationalRiskAtHour(
  regions: Record<string, RegionForecast>,
  hourIdx: number,
): number {
  const vals = Object.values(regions)
    .map((r) => r.hourly_data[hourIdx]?.probability ?? 0)
    .sort((a, b) => a - b);
  if (!vals.length) return 0;
  const idx = Math.min(Math.ceil(vals.length * 0.9) - 1, vals.length - 1);
  return Math.round(vals[idx]);
}

function getWeatherAtHour(regions: Record<string, RegionForecast>, hourIdx: number) {
  const kyiv = regions["Kyiv Oblast"] ?? regions["City of Kyiv"] ?? Object.values(regions)[0];
  return kyiv?.hourly_data[hourIdx]?.weather ?? null;
}

function getHourLabel(regions: Record<string, RegionForecast>, hourIdx: number): string {
  const r = Object.values(regions)[0];
  return r?.hourly_data[hourIdx]?.hour ?? `${String(hourIdx).padStart(2, "0")}:00`;
}

export default function ForecastTimeline({
  forecast,
  selectedHour,
  liveHour,
  onHourChange,
  isPlaying,
  onTogglePlay,
  selectedRegion,
}: ForecastTimelineProps) {
  const { regions, global_metrics } = forecast;
  const isLive = selectedHour === liveHour;

  const playRef = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    if (isPlaying) {
      playRef.current = setInterval(() => onHourChange((prev) => (prev + 1) % 24), 700);
    } else if (playRef.current) {
      clearInterval(playRef.current);
    }
    return () => { if (playRef.current) clearInterval(playRef.current); };
  }, [isPlaying, onHourChange]);

  const getProb = useCallback(
    (hourIdx: number) =>
      selectedRegion && regions[selectedRegion]
        ? (regions[selectedRegion].hourly_data[hourIdx]?.probability ?? 0)
        : nationalRiskAtHour(regions, hourIdx),
    [regions, selectedRegion],
  );

  const bars = Array.from({ length: 24 }, (_, i) => ({
    prob:    getProb(i),
    color:   probToColor(getProb(i)),
    label:   getHourLabel(regions, i),
    weather: getWeatherAtHour(regions, i),
    isAlarm: selectedRegion
      ? (regions[selectedRegion]?.hourly_data[i]?.alarm ?? false)
      : Object.values(regions).some((r) => r.hourly_data[i]?.alarm),
  }));

  const currentWeather = getWeatherAtHour(regions, selectedHour);
  const currentLabel   = getHourLabel(regions, selectedHour);

  return (
    <div
      className="flex flex-col gap-2 px-4 py-3 rounded-xl border"
      style={{
        background:     "rgba(5, 10, 22, 0.85)",
        borderColor:    "rgba(0, 200, 255, 0.1)",
        backdropFilter: "blur(12px)",
      }}
    >

      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-1 h-3 rounded-full bg-cyan-400 animate-pulse" />
          <span className="text-[9px] font-black uppercase tracking-[0.25em]" style={{ color: "#4fc3f7" }}>
            {selectedRegion
              ? selectedRegion.replace(" Oblast", "").replace("City of ", "").toUpperCase()
              : "NATIONAL 90th-PCT RISK"
            }{" "}— 24H FORECAST
          </span>
          {!selectedRegion && !global_metrics.weather_live && (
            <span
              className="text-[8px] font-bold tracking-wider px-1.5 py-0.5 rounded"
              style={{ color: "#fbbf24", background: "rgba(251,191,36,0.08)", border: "1px solid rgba(251,191,36,0.25)" }}
            >
              ⚠ WEATHER OFFLINE
            </span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {currentWeather && (
            <div className="flex items-center gap-1.5 text-[10px] font-mono" style={{ color: "#94a3b8" }}>
              <WeatherIcon icon={currentWeather.icon} size={13} />
              <span>{currentWeather.temp}°</span>
              <span className="opacity-50">|</span>
              <span>💨 {currentWeather.wind} km/h</span>
            </div>
          )}
          <span className="text-[11px] font-black font-mono" style={{ color: "#00e5ff", textShadow: "0 0 10px #00e5ff" }}>
            {currentLabel}
          </span>
        </div>
      </div>

      <div className="flex gap-[3px] items-end h-12">
        {bars.map(({ prob, color, label, weather, isAlarm }, i) => {
          const isSelected = i === selectedHour;
          return (
            <motion.div
              key={i}
              onClick={() => onHourChange(i)}
              whileHover={{ scale: 1.12, y: -2 }}
              whileTap={{ scale: 0.92 }}
              className="flex-1 flex flex-col items-center justify-end cursor-pointer relative"
              title={`${label} · ${prob}%`}
            >
              {weather && (
                <div className={`mb-0.5 transition-opacity duration-200 ${isSelected ? "opacity-100" : "opacity-40"}`}>
                  <WeatherIcon icon={weather.icon} size={10} />
                </div>
              )}
              <motion.div
                className="w-full rounded-sm relative overflow-hidden"
                animate={{
                  height:  Math.max(4, Math.round((prob / 100) * 36)),
                  opacity: isSelected ? 1 : prob > 0 ? 0.6 : 0.2,
                }}
                transition={{ duration: 0.15 }}
                style={{
                  backgroundColor: hexToRgba(color, isSelected ? 0.9 : 0.55),
                  boxShadow: isSelected ? `0 0 8px ${color}, 0 0 2px ${color}` : isAlarm ? `0 0 4px ${color}` : "none",
                  outline:       isSelected ? `1.5px solid ${color}` : "none",
                  outlineOffset: "1px",
                }}
              >
                {isSelected && (
                  <div
                    className="absolute inset-0 opacity-40"
                    style={{
                      background: "linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.5) 50%, transparent 100%)",
                      animation:  "scanShine 1.5s ease-in-out infinite",
                    }}
                  />
                )}
              </motion.div>
              {i % 4 === 0 && (
                <span className="text-[7px] font-mono mt-0.5 opacity-50 absolute -bottom-3.5" style={{ color: "#64748b" }}>
                  {label.includes("T") ? label.slice(11, 13) : label.slice(0, 2)}h
                </span>
              )}
              {isAlarm && (
                <div className="absolute -top-0.5 w-1 h-1 rounded-full" style={{ backgroundColor: "#ff1a3d", boxShadow: "0 0 4px #ff1a3d" }} />
              )}
            </motion.div>
          );
        })}
      </div>

      <div className="flex items-center gap-2 mt-3">

        <motion.button
          onClick={onTogglePlay}
          whileHover={{ scale: 1.1 }}
          whileTap={{ scale: 0.9 }}
          className="flex-none flex items-center justify-center w-7 h-7 rounded-full border transition-colors duration-200"
          style={{
            borderColor: isPlaying ? "#ff1a3d" : "rgba(0,229,255,0.3)",
            background:  isPlaying ? "rgba(255,26,61,0.1)" : "rgba(0,229,255,0.05)",
            color:       isPlaying ? "#ff1a3d" : "#00e5ff",
          }}
        >
          {isPlaying ? (
            <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor">
              <rect x="1" y="1" width="3" height="8" rx="1" />
              <rect x="6" y="1" width="3" height="8" rx="1" />
            </svg>
          ) : (
            <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor">
              <path d="M2 1.5l7 3.5-7 3.5V1.5z" />
            </svg>
          )}
        </motion.button>

        <motion.button
          onClick={() => onHourChange(Math.max(0, selectedHour - 1))}
          whileHover={{ scale: 1.15 }}
          whileTap={{ scale: 0.85 }}
          disabled={selectedHour === 0}
          className="flex-none w-6 h-6 rounded border border-white/10 flex items-center justify-center text-slate-500 hover:text-white hover:border-cyan-500/40 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-[9px]"
        >
          ◀
        </motion.button>

        <input
          type="range"
          min={0}
          max={23}
          value={selectedHour}
          onChange={(e) => onHourChange(parseInt(e.target.value))}
          className="flex-1 h-0.5 rounded-full appearance-none cursor-pointer"
          style={{
            background: `linear-gradient(to right, #00e5ff ${(selectedHour / 23) * 100}%, rgba(255,255,255,0.1) ${(selectedHour / 23) * 100}%)`,
          }}
        />

        <motion.button
          onClick={() => onHourChange(Math.min(23, selectedHour + 1))}
          whileHover={{ scale: 1.15 }}
          whileTap={{ scale: 0.85 }}
          disabled={selectedHour === 23}
          className="flex-none w-6 h-6 rounded border border-white/10 flex items-center justify-center text-slate-500 hover:text-white hover:border-cyan-500/40 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-[9px]"
        >
          ▶
        </motion.button>

        <motion.button
          onClick={() => onHourChange(liveHour)}
          whileHover={{ scale: 1.08 }}
          whileTap={{ scale: 0.92 }}
          className={`flex-none px-2.5 py-1 rounded text-[9px] font-black uppercase tracking-widest transition-all ${
            isLive
              ? "bg-red-500/20 border border-red-500/40 text-red-400"
              : "bg-cyan-500/10 border border-cyan-500/20 text-cyan-600 hover:text-cyan-400 hover:bg-cyan-500/20 hover:border-cyan-500/40"
          }`}
        >
          {isLive && (
            <span className="inline-block w-1.5 h-1.5 rounded-full bg-red-500 mr-1 align-middle animate-pulse" />
          )}
          LIVE
        </motion.button>

        <div className="flex items-center gap-1.5 text-[8px] font-mono opacity-40 ml-1">
          <div className="w-2 h-1 rounded-sm" style={{ background: "#004d2e" }} />
          <span style={{ color: "#64748b" }}>SAFE</span>
          <div className="w-2 h-1 rounded-sm" style={{ background: "#ff6b00" }} />
          <span style={{ color: "#64748b" }}>HIGH</span>
          <div className="w-2 h-1 rounded-sm" style={{ background: "#ff1a3d" }} />
          <span style={{ color: "#64748b" }}>CRIT</span>
        </div>
      </div>
    </div>
  );
}