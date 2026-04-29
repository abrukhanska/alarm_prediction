"use client";
import { useState, useMemo } from "react";
import { ComposableMap, Geographies, Geography, Marker } from "react-simple-maps";
import { AnimatePresence, motion } from "framer-motion";
import { SHAPE_TO_BACKEND, SHAPE_NAME_TO_ID, REGION_LABELS } from "@/lib/regions";
import type { RegionForecast } from "@/lib/types";
import { probToColor, hexToRgba, RISK_LEVEL_LABELS } from "@/lib/colors";

const GEO_URL = "/geo/ukraine-adm1.json";

interface UkraineMapProps {
  regions: Record<string, RegionForecast>;
  selectedHour: number;
  selectedRegion: string | null;
  onSelectRegion: (backendName: string) => void;
}

interface TooltipState {
  backendName: string;
  shapeName: string;
  x: number;
  y: number;
}

export default function UkraineMap({
  regions,
  selectedHour,
  selectedRegion,
  onSelectRegion,
}: UkraineMapProps) {
  const [tooltip, setTooltip] = useState<TooltipState | null>(null);

  const handleEnter = (shapeName: string, x: number, y: number) => {
    const backendName = SHAPE_TO_BACKEND[shapeName];
    if (backendName) setTooltip({ backendName, shapeName, x, y });
  };
  const handleLeave = () => setTooltip(null);

  return (
    <div className="relative w-full h-full flex items-center justify-center">
      <div
        className="absolute inset-0 pointer-events-none z-10 opacity-[0.025]"
        style={{
          backgroundImage: "repeating-linear-gradient(0deg, transparent, transparent 3px, rgba(0,229,255,0.5) 3px, rgba(0,229,255,0.5) 4px)",
        }}
      />

      <ComposableMap
        projection="geoMercator"
        projectionConfig={{ center: [31.5, 49.0], scale: 2400 }}
        className="w-full h-full object-contain"
        style={{ filter: "drop-shadow(0 0 30px rgba(0,100,200,0.15))" }}
      >
        <Geographies geography={GEO_URL}>
          {({ geographies }) => (
            <>
              {geographies.map((geo) => {
                const shapeName: string = geo.properties.shapeName ?? geo.properties.NAME_1 ?? "";
                const backendName = SHAPE_TO_BACKEND[shapeName];
                const regionData = backendName ? regions[backendName] : undefined;

                const prob = regionData?.hourly_data[selectedHour]?.probability ?? 0;
                const isLive = regionData?.is_live_alarm_now ?? false;
                const isSelected = selectedRegion === backendName;
                const color = probToColor(prob);

                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={isLive ? hexToRgba("#b91c2a", isSelected ? 0.85 : 0.70) : prob === 0 ? hexToRgba("#1d7a4a", isSelected ? 0.90 : 0.65) : hexToRgba(color, isSelected ? 0.85 : 0.50)}
                    stroke={
                      isLive
                        ? "rgba(160,30,45,0.55)"
                        : isSelected
                        ? "#00e5ff"
                        : "rgba(120,170,210,0.28)"
                    }
                    strokeWidth={isLive ? 1.2 : isSelected ? 2.2 : 0.7}
                    className={isLive ? "live-alarm-region" : ""}
                    style={{
                      default: {
                        outline: "none",
                        transition: "all 0.35s ease",
                        filter: "none",
                      },
                      hover: {
                        outline: "none",
                        fillOpacity: 0.9,
                        cursor: backendName ? "pointer" : "default",
                        stroke: "#00e5ff",
                        filter: "drop-shadow(0 0 4px rgba(0,229,255,0.4))",
                      },
                      pressed: { outline: "none" },
                    }}
                    onMouseEnter={(e) => handleEnter(shapeName, e.clientX, e.clientY)}
                    onMouseMove={(e) =>
                      setTooltip((t) => t ? { ...t, x: e.clientX, y: e.clientY } : null)
                    }
                    onMouseLeave={handleLeave}
                    onClick={() => backendName && onSelectRegion(backendName)}
                  />
                );
              })}

              {geographies.map((geo) => {
                const shapeName: string = geo.properties.shapeName ?? "";
                const slug = SHAPE_NAME_TO_ID[shapeName];
                const markerData = slug ? REGION_LABELS[slug] : undefined;
                if (!markerData) return null;

                const backendName = SHAPE_TO_BACKEND[shapeName];
                const regionData = backendName ? regions[backendName] : undefined;
                const isLive = regionData?.is_live_alarm_now ?? false;

                return (
                  <Marker key={`m-${slug}`} coordinates={markerData.coords}>
                    {isLive && (
                      <g style={{ pointerEvents: "none" }}>
                        <circle r="8" fill="none" stroke="#b91c2a" strokeWidth="1.5" opacity="0.8"
                          style={{ animation: "radarPing 1.8s ease-out infinite", transformBox: "fill-box", transformOrigin: "center" }} />
                        <circle r="8" fill="none" stroke="#b84500" strokeWidth="1" opacity="0.5"
                          style={{ animation: "radarPing 1.8s ease-out infinite 0.6s", transformBox: "fill-box", transformOrigin: "center" }} />
                        <circle r="4" fill="#b91c2a" opacity="0.95"
                          style={{ animation: "pulseCore 1.2s ease-in-out infinite" }} />
                      </g>
                    )}
                    <text
                      textAnchor="middle"
                      y={isLive ? 14 : 2}
                      style={{
                        fontSize: slug === "kyiv_oblast" || slug === "kyiv" ? "6.5px" : "5.5px",
                        fontFamily: "'Share Tech Mono', monospace",
                        fontWeight: "bold",
                        fill: isLive ? "#d96060" : "rgba(255,255,255,0.55)",
                        pointerEvents: "none",
                        letterSpacing: "0.05em",
                      }}
                    >
                      {markerData.name}
                    </text>
                  </Marker>
                );
              })}
            </>
          )}
        </Geographies>
      </ComposableMap>

      <AnimatePresence>
        {tooltip && (() => {
          const rd = regions[tooltip.backendName];
          const prob = rd?.hourly_data[selectedHour]?.probability ?? 0;
          const weather = rd?.hourly_data[selectedHour]?.weather;
          const color = probToColor(prob);
          const isLive = rd?.is_live_alarm_now ?? false;

          return (
            <motion.div
              key="tooltip"
              initial={{ opacity: 0, scale: 0.92, y: 4 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.9 }}
              transition={{ duration: 0.12 }}
              className="pointer-events-none fixed z-50 rounded-lg px-3 py-2.5 text-xs shadow-2xl"
              style={{
                left: tooltip.x + 14,
                top: tooltip.y - 14,
                background: "rgba(5, 10, 22, 0.95)",
                border: `1px solid ${hexToRgba(color, 0.5)}`,
                boxShadow: `0 0 20px ${hexToRgba(color, 0.2)}`,
                minWidth: 160,
                fontFamily: "'Share Tech Mono', monospace",
              }}
            >

              <div className="font-bold text-white text-sm mb-1.5 flex items-center gap-2">
                {isLive && (
                  <span
                    className="inline-block w-1.5 h-1.5 rounded-full animate-pulse"
                    style={{ background: "#b91c2a" }}
                  />
                )}
                {tooltip.backendName.replace(" Oblast", "").replace("City of ", "")}
                {isLive && (
                  <span className="text-[9px] font-bold tracking-wider" style={{ color: "#b91c2a" }}>
                    ● ALARM ACTIVE
                  </span>
                )}
              </div>

              <div className="flex items-center gap-2 mb-1">
                <div className="flex-1 h-1.5 rounded-full" style={{ background: "rgba(255,255,255,0.08)" }}>
                  <div
                    className="h-full rounded-full"
                    style={{ width: `${prob}%`, background: color, boxShadow: `0 0 4px ${color}` }}
                  />
                </div>
                <span style={{ color, minWidth: 32, textAlign: "right" }}>{prob}%</span>
              </div>

              {weather && (
                <div className="text-[10px] flex gap-3 opacity-70 text-slate-300 mt-1.5">
                  <span>🌡 {weather.temp}°</span>
                  <span>💨 {weather.wind}km/h</span>
                  <span>☁ {weather.cloudcover}%</span>
                </div>
              )}

              {rd && (
                <div className="text-[9px] mt-1 opacity-50 text-slate-400">
                  24h max: {Math.round(rd.max_probability * 100)}% · {RISK_LEVEL_LABELS[rd.risk_level] ?? rd.risk_level}
                </div>
              )}
            </motion.div>
          );
        })()}
      </AnimatePresence>

      <div
        className="absolute bottom-3 right-3 z-20 rounded-lg p-2.5 border"
        style={{
          background: "rgba(3, 7, 18, 0.85)",
          borderColor: "rgba(0,229,255,0.1)",
          backdropFilter: "blur(8px)",
        }}
      >
        <div className="text-[7.5px] font-black tracking-[0.2em] uppercase mb-1.5" style={{ color: "#4fc3f7" }}>
          THREAT LEVEL
        </div>
        {[
          { label: "SAFE",     color: "#004d2e" },
          { label: "LOW",      color: "#66dd00" },
          { label: "MEDIUM",   color: "#ffc800" },
          { label: "HIGH",     color: "#b84500" },
          { label: "CRITICAL", color: "#b91c2a" },
        ].map(({ label, color }) => (
          <div key={label} className="flex items-center gap-2 mb-0.5">
            <div
              className="w-2 h-2 rounded-sm"
              style={{ background: color, boxShadow: `0 0 4px ${color}44` }}
            />
            <span className="text-[7px] font-bold tracking-wider" style={{ color: "#64748b" }}>
              {label}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}