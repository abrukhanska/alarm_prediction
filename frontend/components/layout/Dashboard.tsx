"use client";
import { useState, useEffect, useCallback } from "react";
import Header from "./Header";
import UkraineMap from "@/components/map/UkraineMap";
import RegionPanel from "@/components/panels/RegionPanel";
import { fetchCurrentAlarms, fetchStats } from "@/lib/api";
import { AlarmsResponse, StatsResponse } from "@/lib/types";

export default function Dashboard() {
  const [alarms, setAlarms] = useState<AlarmsResponse | null>(null);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);

  const loadAlarms = useCallback(async () => {
    try {
      const data = await fetchCurrentAlarms();
      setAlarms(data);
    } catch (e) { console.error(e); }
  }, []);

  const loadStats = useCallback(async () => {
    try {
      const data = await fetchStats();
      setStats(data);
    } catch (e) { console.error(e); }
  }, []);

  useEffect(() => {
    loadAlarms(); loadStats();
    const id = setInterval(() => { loadAlarms(); loadStats(); }, 30000);
    return () => clearInterval(id);
  }, [loadAlarms, loadStats]);

  return (
    <div className="relative flex flex-col h-screen w-screen overflow-hidden bg-[#05070a] text-slate-200 font-sans">
      <div className="absolute inset-0 z-0 opacity-[0.03] pointer-events-none bg-[linear-gradient(#06b6d4_1px,transparent_1px),linear-gradient(90deg,#06b6d4_1px,transparent_1px)] bg-[length:40px_40px]" />
      <div className="absolute inset-0 z-0 pointer-events-none bg-[radial-gradient(circle_at_center,transparent_0%,rgba(5,7,10,0.9)_100%)]" />

      <div className="flex-none z-50 relative border-b border-white/5 bg-slate-950/60 backdrop-blur-md">
        <Header stats={stats} />
      </div>

      <div className="flex-1 relative overflow-hidden flex flex-row">
        <div className="flex-1 relative z-20 p-4 pb-12">
          <UkraineMap
              alarms={alarms?.regions ?? []}
              selectedRegion={selectedRegion}
              onSelectRegion={setSelectedRegion}
          />

          <div className="absolute bottom-12 right-8 z-30 hidden md:block">
            <div className="glass-panel p-3 rounded-xl border border-white/10 bg-slate-950/40 backdrop-blur-xl shadow-2xl min-w-[130px]">
              <div className="flex items-center gap-2 mb-2">
                <div className="w-1 h-3 bg-cyan-500 rounded-full" />
                <h3 className="text-[9px] font-black uppercase tracking-[0.15em] text-slate-400">Threat Level</h3>
              </div>
              <div className="space-y-1">
                {[{ label: "Safe", color: "#10b981" }, { label: "Low", color: "#84cc16" }, { label: "Medium", color: "#f59e0b" }, { label: "High", color: "#f97316" }, { label: "Critical", color: "#ef4444" }].map((item) => (
                  <div key={item.label} className="flex items-center">
                    <div className="w-2 h-2 rounded-sm" style={{ backgroundColor: item.color, boxShadow: `0 0 6px ${item.color}33` }} />
                    <span className="ml-3 text-[8px] font-bold uppercase tracking-wider text-slate-400">{item.label}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        <RegionPanel regionId={selectedRegion} onClose={() => setSelectedRegion(null)} />
      </div>

      {/* SLIM FORECAST BAR */}
      <div className="absolute bottom-4 left-1/2 -translate-x-1/2 w-[92%] z-40">
        <div className="glass-panel rounded-md border border-white/10 bg-slate-950/40 backdrop-blur-xl overflow-hidden">
          <div className="px-4 py-1.5 flex items-center justify-between text-[9px] font-black uppercase tracking-[0.2em] text-slate-400">
            <div className="flex items-center gap-2">
               <div className="w-1 h-1 bg-cyan-500 rounded-full animate-pulse" />
               <span>24H Strategic Forecast</span>
            </div>
            <div className="h-[1px] flex-1 mx-8 bg-gradient-to-r from-transparent via-white/10 to-transparent" />
            {selectedRegion && <span className="text-cyan-500/60 tracking-tighter italic">Live Stream Active</span>}
          </div>
        </div>
      </div>
    </div>
  );
}