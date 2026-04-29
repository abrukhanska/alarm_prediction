"use client";
import { useState, useEffect, useCallback, useRef } from "react";
import type { ForecastResponse, RegionForecast } from "@/lib/types";
import { fetchForecast } from "@/lib/api";

import Header from "./Header";
import NationalRiskGauge from "@/components/gauge/Nationalriskgauge";
import UkraineMap from "@/components/map/UkraineMap";
import ForecastTimeline from "@/components/timeline/ForecastTimeline";
import RegionDrawer from "@/components/panels/RegionDrawer";
import MLOpsTerminal from "@/components/panels/MLOpsTerminal";

const REFRESH_INTERVAL_MS = 5 * 60 * 1000;

export default function Dashboard() {
  const [forecast, setForecast] = useState<ForecastResponse | null>(null);
  const [error, setError]       = useState<string | null>(null);
  const [loading, setLoading]   = useState(true);

  const [selectedHour, setSelectedHour]     = useState(0);
  const [isPlaying, setIsPlaying]           = useState(false);
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const [mlopsOpen, setMlopsOpen]           = useState(false);
  const [liveHour, setLiveHour]             = useState(0);

  const load = useCallback(async () => {
    try {
      const data = await fetchForecast();
      setForecast(data);
      setError(null);

      const kyivHour = parseInt(
        new Intl.DateTimeFormat("en-US", {
          timeZone: "Europe/Kyiv",
          hour: "numeric",
          hour12: false,
        }).format(new Date()),
        10,
      );
      const firstRegion = Object.values(data.regions)[0];
      if (firstRegion) {
        const idx = firstRegion.hourly_data.findIndex((h) => {
          const raw = h.hour.includes("T") ? h.hour.slice(11, 13) : h.hour.slice(0, 2);
          return parseInt(raw, 10) === kyivHour;
        });
        const kyivHourVal = idx >= 0 ? idx : 0;
        setSelectedHour(kyivHourVal);
        setLiveHour(kyivHourVal);
      }
    } catch (e: any) {
      setError(e.message ?? "Failed to load forecast");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, [load]);

  const handleHourChange = useCallback((h: number | ((prev: number) => number)) => {
    setSelectedHour(h as any);
  }, []);

  const handleTogglePlay = useCallback(() => {
    setIsPlaying((p) => !p);
  }, []);

  const handleSelectRegion = useCallback((backendName: string) => {
    setSelectedRegion((prev) => (prev === backendName ? null : backendName));
  }, []);

  if (loading) {
    return (
      <div
        className="flex h-screen w-screen items-center justify-center"
        style={{ background: "#050e1f", fontFamily: "'Share Tech Mono', monospace" }}
      >
        <div className="flex flex-col items-center gap-4">
          <div
            className="text-3xl"
            style={{ filter: "drop-shadow(0 0 12px rgba(0,229,255,0.7))" }}
          >
            🛡️
          </div>
          <div className="text-[11px] tracking-[0.3em] uppercase animate-pulse" style={{ color: "#4fc3f7" }}>
            AEGIS — INITIALIZING INTELLIGENCE FEED…
          </div>
          <div className="w-48 h-0.5 rounded-full overflow-hidden" style={{ background: "rgba(255,255,255,0.05)" }}>
            <div
              className="h-full rounded-full"
              style={{
                background: "linear-gradient(90deg, #00e5ff, #0077ff)",
                animation: "loadBar 1.5s ease-in-out infinite",
                width: "40%",
              }}
            />
          </div>
        </div>
      </div>
    );
  }

  if (error || !forecast) {
    return (
      <div
        className="flex h-screen w-screen items-center justify-center"
        style={{ background: "#050e1f", fontFamily: "'Share Tech Mono', monospace" }}
      >
        <div
          className="rounded-xl p-6 text-center max-w-md"
          style={{
            background: "rgba(255,26,61,0.06)",
            border: "1px solid rgba(255,26,61,0.3)",
          }}
        >
          <div className="text-2xl mb-3">⚠</div>
          <div className="text-sm font-bold mb-2" style={{ color: "#ff6680" }}>
            INTELLIGENCE FEED OFFLINE
          </div>
          <div className="text-[10px] mb-4" style={{ color: "#64748b" }}>
            {error ?? "No forecast data available."}
            <br />
            Ensure backend is running and POST /api/update-forecast was called.
          </div>
          <button
            onClick={load}
            className="px-4 py-2 rounded-lg text-[10px] font-black tracking-widest uppercase border transition-all"
            style={{
              borderColor: "rgba(0,229,255,0.3)",
              color: "#00e5ff",
              background: "rgba(0,229,255,0.05)",
            }}
          >
            ⟳ RETRY CONNECTION
          </button>
        </div>
      </div>
    );
  }

  const { global_metrics, regions } = forecast;
  const regionData = selectedRegion ? regions[selectedRegion] ?? null : null;

  return (
    <div
      className="relative flex flex-col h-screen w-screen overflow-hidden"
      style={{ background: "#050e1f", color: "#e2e8f0" }}
    >
      <div
        className="absolute inset-0 pointer-events-none z-0 opacity-[0.022]"
        style={{
          backgroundImage: `
            linear-gradient(rgba(0,229,255,0.6) 1px, transparent 1px),
            linear-gradient(90deg, rgba(0,229,255,0.6) 1px, transparent 1px)
          `,
          backgroundSize: "44px 44px",
        }}
      />
      <div
        className="absolute inset-0 pointer-events-none z-0"
        style={{
          background: "radial-gradient(ellipse at center, transparent 40%, rgba(3,7,18,0.85) 100%)",
        }}
      />

      <div className="relative z-30 flex-none">
        <Header
          metrics={global_metrics}
          onOpenMLOps={() => setMlopsOpen(true)}
        />
      </div>

      <div className="relative z-10 flex-1 overflow-hidden" style={{ minHeight: 0 }}>
        {/* Gauge overlay — top-left corner above the map */}
        <div
          style={{
            position: "absolute",
            top: 12,
            left: 16,
            zIndex: 20,
            pointerEvents: "none",
          }}
        >
          <NationalRiskGauge
            value={global_metrics.national_risk_index ?? 0}
            liveAlarms={global_metrics.live_alarms_count ?? 0}
            regionsAtRisk={global_metrics.total_regions_at_risk ?? 0}
          />
        </div>
        <UkraineMap
          regions={regions}
          selectedHour={selectedHour}
          selectedRegion={selectedRegion}
          onSelectRegion={handleSelectRegion}
        />
      </div>

      <div className="relative z-20 flex-none px-4 pb-4 pt-0">
        <ForecastTimeline
          forecast={forecast}
          selectedHour={selectedHour}
          liveHour={liveHour}
          onHourChange={handleHourChange}
          isPlaying={isPlaying}
          onTogglePlay={handleTogglePlay}
          selectedRegion={selectedRegion}
        />
      </div>

      <RegionDrawer
        regionName={selectedRegion}
        regionData={regionData}
        selectedHour={selectedHour}
        onClose={() => setSelectedRegion(null)}
      />

      <MLOpsTerminal
        metrics={global_metrics}
        isOpen={mlopsOpen}
        onClose={() => setMlopsOpen(false)}
        onForecastUpdated={load}
      />
    </div>
  );
}