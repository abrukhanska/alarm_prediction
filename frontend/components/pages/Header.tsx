"use client";
import { useEffect, useState } from "react";
import type { GlobalMetrics } from "@/lib/types";
import AnimatedNumber from "@/components/ui/AnimatedNumber";
import PulsingDot from "@/components/ui/PulsingDot";

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
        }) + " KYIV",
      );
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <header
      style={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "0 24px",
        background: "rgba(3, 7, 18, 0.97)",
        borderBottom: "1px solid rgba(0,229,255,0.12)",
        backdropFilter: "blur(16px)",
        fontFamily: "'IBM Plex Mono', monospace",
        height: 52,
        flexShrink: 0,
        position: "relative",
        zIndex: 30,
      }}
    >
      {/* LEFT: Logo */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <span style={{ fontSize: 20, filter: "drop-shadow(0 0 8px rgba(0,229,255,0.8))" }}>
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
              textShadow: "0 0 16px rgba(0,229,255,0.7)",
              lineHeight: 1,
            }}
          >
            AEGIS
          </div>
          <div
            style={{
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: 7.5,
              letterSpacing: "0.22em",
              color: "#4a7fa5",
              textTransform: "uppercase",
              marginTop: 1,
            }}
          >
            INTELLIGENCE SYSTEM
          </div>
        </div>
      </div>

      {/* RIGHT: metrics + mlops + clock */}
      <div style={{ display: "flex", flexDirection: "row", alignItems: "center", gap: 18 }}>
        {metrics && (
          <>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <PulsingDot color="#ff1a3d" size={7} />
              <span style={{ color: "#64748b", fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase" }}>
                ALARMS
              </span>
              <span style={{ color: "#ff4060", fontWeight: 700, fontSize: 13 }}>
                <AnimatedNumber value={metrics.live_alarms_count} />
              </span>
            </div>

            <div style={{ width: 1, height: 18, background: "rgba(255,255,255,0.09)" }} />

            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#fb923c" }} />
              <span style={{ color: "#64748b", fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase" }}>
                AT RISK
              </span>
              <span style={{ color: "#fb923c", fontWeight: 700, fontSize: 13 }}>
                <AnimatedNumber value={metrics.total_regions_at_risk} />
              </span>
            </div>

            <div style={{ width: 1, height: 18, background: "rgba(255,255,255,0.09)" }} />
          </>
        )}

        <button
          onClick={onOpenMLOps}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 5,
            padding: "4px 11px",
            borderRadius: 6,
            border: "1px solid rgba(255,26,61,0.4)",
            background: "rgba(255,26,61,0.07)",
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
            (e.currentTarget as HTMLButtonElement).style.background = "rgba(255,26,61,0.14)";
            (e.currentTarget as HTMLButtonElement).style.borderColor = "rgba(255,26,61,0.7)";
          }}
          onMouseLeave={e => {
            (e.currentTarget as HTMLButtonElement).style.background = "rgba(255,26,61,0.07)";
            (e.currentTarget as HTMLButtonElement).style.borderColor = "rgba(255,26,61,0.4)";
          }}
        >
          ⚡ MLOPS
        </button>

        <div
          style={{
            fontFamily: "'IBM Plex Mono', monospace",
            fontWeight: 600,
            fontSize: 13,
            color: "#00e5ff",
            textShadow: "0 0 10px rgba(0,229,255,0.5)",
            letterSpacing: "0.08em",
            fontVariantNumeric: "tabular-nums",
            minWidth: 128,
            textAlign: "right",
          }}
        >
          {clock}
        </div>
      </div>
    </header>
  );
}
