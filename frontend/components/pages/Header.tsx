"use client";
import { useEffect, useState } from "react";
import type { GlobalMetrics } from "@/lib/types";
import { riskIndexToColor, hexToRgba } from "@/lib/colors";
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
  const riskColor = riskIndexToColor(risk);

  return (
    <header
      style={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "6px 20px",
        background: "rgba(3, 7, 18, 0.97)",
        borderBottom: "1px solid rgba(0,229,255,0.08)",
        backdropFilter: "blur(16px)",
        fontFamily: "'IBM Plex Mono', monospace",
        height: 64,
        flexShrink: 0,
        position: "relative",
        zIndex: 30,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 140 }}>
        <span style={{ fontSize: 22, filter: "drop-shadow(0 0 8px rgba(0,229,255,0.7))" }}>
          🛡️
        </span>
        <div>
          <div
            style={{
              fontFamily: "'Barlow Condensed', sans-serif",
              fontWeight: 900,
              fontSize: 22,
              letterSpacing: "0.14em",
              color: "#00e5ff",
              textShadow: "0 0 14px rgba(0,229,255,0.55)",
              lineHeight: 1,
            }}
          >
            AEGIS
          </div>
          <div
            style={{
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: 8,
              letterSpacing: "0.22em",
              color: "#2d4a6b",
              textTransform: "uppercase",
              marginTop: 1,
            }}
          >
            INTELLIGENCE SYSTEM
          </div>
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1 }}>
        <NationalRiskGauge
          value={risk}
          liveAlarms={metrics?.live_alarms_count ?? 0}
          regionsAtRisk={metrics?.total_regions_at_risk ?? 0}
        />
      </div>

      <div
        style={{
          display: "flex",
          flexDirection: "row",
          alignItems: "center",
          gap: 20,
          minWidth: 140,
          justifyContent: "flex-end",
        }}
      >
        {metrics && (
          <>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <PulsingDot color="#ff1a3d" size={7} />
              <span style={{ color: "#4a5568", fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase" }}>
                ALARMS
              </span>
              <span style={{ color: "#ff1a3d", fontWeight: 700, fontSize: 12 }}>
                <AnimatedNumber value={metrics.live_alarms_count} />
              </span>
            </div>

            <div style={{ width: 1, height: 20, background: "rgba(255,255,255,0.07)" }} />

            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#f97316" }} />
              <span style={{ color: "#4a5568", fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase" }}>
                AT RISK
              </span>
              <span style={{ color: "#f97316", fontWeight: 700, fontSize: 12 }}>
                <AnimatedNumber value={metrics.total_regions_at_risk} />
              </span>
            </div>

            {/* Divider */}
            <div style={{ width: 1, height: 20, background: "rgba(255,255,255,0.07)" }} />
          </>
        )}

        <button
          onClick={onOpenMLOps}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 5,
            padding: "5px 12px",
            borderRadius: 6,
            border: "1px solid rgba(255,26,61,0.35)",
            background: "rgba(255,26,61,0.06)",
            color: "#ff6680",
            fontFamily: "'IBM Plex Mono', monospace",
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.18em",
            textTransform: "uppercase",
            cursor: "pointer",
            transition: "all 0.15s",
          }}
          onMouseEnter={e => {
            (e.currentTarget as HTMLButtonElement).style.background = "rgba(255,26,61,0.12)";
            (e.currentTarget as HTMLButtonElement).style.borderColor = "rgba(255,26,61,0.6)";
          }}
          onMouseLeave={e => {
            (e.currentTarget as HTMLButtonElement).style.background = "rgba(255,26,61,0.06)";
            (e.currentTarget as HTMLButtonElement).style.borderColor = "rgba(255,26,61,0.35)";
          }}
        >
          ⚡ MLOPS
        </button>

        <div
          style={{
            fontFamily: "'IBM Plex Mono', monospace",
            fontWeight: 600,
            fontSize: 14,
            color: "#00e5ff",
            textShadow: "0 0 10px rgba(0,229,255,0.4)",
            letterSpacing: "0.08em",
            fontVariantNumeric: "tabular-nums",
            minWidth: 120,
            textAlign: "right",
          }}
        >
          {clock}
        </div>
      </div>
    </header>
  );
}