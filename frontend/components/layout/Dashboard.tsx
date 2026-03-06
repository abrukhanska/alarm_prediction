"use client";
import { useState, useEffect, useCallback } from "react";
import Header from "./Header";
import UkraineMap from "@/components/map/UkraineMap";
import RegionPanel from "@/components/panels/RegionPanel";
import ForecastTimeline from "@/components/timeline/ForecastTimeline";
import { fetchCurrentAlarms, fetchStats, fetchTimeline } from "@/lib/api";
import { AlarmsResponse, StatsResponse, TimelineHour } from "@/lib/types";

export default function Dashboard() {
  const [alarms, setAlarms] = useState<AlarmsResponse | null>(null);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const [timeline, setTimeline] = useState<TimelineHour[]>([]);
  const [currentHour, setCurrentHour] = useState(new Date().getUTCHours());

  const loadAlarms = useCallback(async () => {
    try {
      const data = await fetchCurrentAlarms();
      setAlarms(data);
    } catch (e) {
      console.error("Failed to load alarms", e);
    }
  }, []);

  const loadStats = useCallback(async () => {
    try {
      const data = await fetchStats();
      setStats(data);
    } catch (e) {
      console.error("Failed to load stats", e);
    }
  }, []);

  const loadTimeline = useCallback(async (region: string) => {
    try {
      const data = await fetchTimeline(region);
      setTimeline(data.hours);
    } catch (e) {
      console.error("Failed to load timeline", e);
    }
  }, []);

  useEffect(() => {
    loadAlarms();
    loadStats();
    const id = setInterval(() => {
      loadAlarms();
      loadStats();
    }, 30000);
    return () => clearInterval(id);
  }, [loadAlarms, loadStats]);

  useEffect(() => {
    if (selectedRegion) {
      loadTimeline(selectedRegion);
    } else {
      setTimeline([]);
    }
  }, [selectedRegion, loadTimeline]);

  useEffect(() => {
    const id = setInterval(() => setCurrentHour(new Date().getUTCHours()), 60000);
    return () => clearInterval(id);
  }, []);

  const handleSelectRegion = useCallback((id: string) => {
    setSelectedRegion((prev) => (prev === id ? null : id));
  }, []);

  const handleClosePanel = useCallback(() => {
    setSelectedRegion(null);
  }, []);

  return (
      <div
          className="flex flex-col h-screen w-screen overflow-hidden text-slate-200"
      >
        <div className="flex-none z-20 relative glass-panel">
          <Header stats={stats} />
        </div>

        <div className="flex flex-1 overflow-hidden relative">
          <div className="flex-1 relative w-full h-full p-2">
            <UkraineMap
                alarms={alarms?.regions ?? []}
                selectedRegion={selectedRegion}
                onSelectRegion={handleSelectRegion}
            />
          </div>

          <RegionPanel
              regionId={selectedRegion}
              onClose={handleClosePanel}
          />
        </div>

        <div
            className="flex-none border-t glass-panel flex flex-col h-36 z-20 relative"
            style={{ borderColor: "rgba(30, 58, 95, 0.4)" }}
        >
          <div className="px-5 py-2.5 flex items-center gap-3">
          <span className="text-[11px] font-bold uppercase tracking-widest text-slate-500">
            24h Forecast Timeline
          </span>
            {selectedRegion && (
                <span className="text-sm font-semibold" style={{ color: "#06b6d4" }}>
              — {selectedRegion.replace(/_/g, " ")}
            </span>
            )}
          </div>

          <div className="flex-1 px-5 pb-3 h-full">
            <ForecastTimeline hours={timeline} currentHour={currentHour} />
          </div>
        </div>
      </div>
  );
}