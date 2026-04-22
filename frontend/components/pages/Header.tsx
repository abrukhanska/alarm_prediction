"use client";
import { useEffect, useState } from "react";
import type { GlobalMetrics } from "@/lib/types";
import { riskIndexToColor, hexToRgba, probToLevel } from "@/lib/colors";
import AnimatedNumber from "@/components/ui/AnimatedNumber";
import PulsingDot from "@/components/ui/PulsingDot";
import NationalRiskGauge from "@/components/gauge/Nationalriskgauge";

interface HeaderProps {
  metrics: GlobalMetrics | null;
  onOpenMLOps: () => void;
}

export default function Header({ metrics, onOpenMLOps }: HeaderProps) {
  const [clock, setClock] = useState("");

  useEffect(() => {
    const tick = () => {
      const now = new Date();
      setClock(
        now.toLocaleTimeString("en-GB", {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          timeZone: "Europe/Kyiv",
        }) + " KYV",
      );
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  const risk = metrics?.national_risk_index ?? 0;

  return (
    <header
      className="flex items-center justify-between px-5 py-2"
      style={{
        background: "rgba(3, 7, 18, 0.95)",
        borderBottom: "1px solid rgba(0,229,255,0.08)",
        backdropFilter: "blur(12px)",
        fontFamily: "'Share Tech Mono', monospace",
      }}
    >
      <div className="flex items-center gap-3 min-w-[160px]">
        <div
          className="text-2xl"
          style={{ filter: "drop-shadow(0 0 8px rgba(0,229,255,0.7))" }}
        >
          🛡️
        </div>
        <div>
          <div
            className="font-black text-lg tracking-[0.12em]"
            style={{
              color: "#00e5ff",
              textShadow: "0 0 12px rgba(0,229,255,0.5)",
              fontFamily: "'Rajdhani', sans-serif",
            }}
          >
            AEGIS
          </div>
          <div className="text-[8px] tracking-[0.18em] uppercase" style={{ color: "#334155" }}>
            INTELLIGENCE SYSTEM
          </div>
        </div>
      </div>

      <div className="flex items-center justify-center min-w-[220px]">
        <NationalRiskGauge
          value={risk}
          liveAlarms={metrics?.live_alarms_count ?? 0}
          regionsAtRisk={metrics?.total_regions_at_risk ?? 0}
        />
      </div>

      <div className="flex items-center gap-5 text-[10px]">
        {metrics && (
          <>
            <div className="flex items-center gap-1.5">
              <PulsingDot color="#ff1a3d" size={7} />
              <span style={{ color: "#64748b" }}>ALARMS</span>
              <span className="font-bold" style={{ color: "#ff1a3d" }}>
                <AnimatedNumber value={metrics.live_alarms_count} />
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="w-1.5 h-1.5 rounded-full" style={{ background: "#f97316" }} />
              <span style={{ color: "#64748b" }}>AT RISK</span>
              <span className="font-bold" style={{ color: "#f97316" }}>
                <AnimatedNumber value={metrics.total_regions_at_risk} />
              </span>
            </div>
          </>
        )}

        <button
          onClick={onOpenMLOps}
          className="flex items-center gap-1.5 px-2.5 py-1 rounded border text-[9px] font-black tracking-[0.15em] uppercase transition-all duration-150 hover:brightness-125"
          style={{
            borderColor: "rgba(255,26,61,0.35)",
            background: "rgba(255,26,61,0.06)",
            color: "#ff6680",
          }}
        >
          ⚡ MLOPS
        </button>

        <div
          className="font-black tracking-[0.1em] text-base text-right min-w-[130px]"
          style={{
            color: "#00e5ff",
            textShadow: "0 0 8px rgba(0,229,255,0.4)",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          {clock}
        </div>
      </div>
    </header>
  );
}