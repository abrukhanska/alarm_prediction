"use client";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { GlobalMetrics } from "@/lib/types";
import { triggerRetrain, triggerForecastUpdate } from "@/lib/api";

interface MLOpsTerminalProps {
  metrics: GlobalMetrics;
  isOpen: boolean;
  onClose: () => void;
  onForecastUpdated?: () => void;
}

interface LogLine {
  ts: string;
  msg: string;
  type: "info" | "success" | "error" | "warn";
}

function fmt(iso: string): string {
  try {
    return new Date(iso).toLocaleString("en-GB", {
      day: "2-digit", month: "short", year: "numeric",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  } catch {
    return iso;
  }
}

const TYPE_COLORS: Record<string, string> = {
  info:    "#4fc3f7",
  success: "#4ade80",
  error:   "#ff1a3d",
  warn:    "#fbbf24",
};

export default function MLOpsTerminal({ metrics, isOpen, onClose, onForecastUpdated }: MLOpsTerminalProps) {
  const [logs, setLogs] = useState<LogLine[]>([
    {
      ts: new Date().toISOString(),
      msg: `System initialized · model updated ${fmt(metrics.last_model_update)}`,
      type: "info",
    },
  ]);
  const [retraining, setRetraining] = useState(false);
  const [updating, setUpdating] = useState(false);

  const addLog = (msg: string, type: LogLine["type"] = "info") => {
    setLogs((prev) => [
      ...prev,
      { ts: new Date().toISOString(), msg, type },
    ].slice(-30));
  };

  const handleRetrain = async () => {
    setRetraining(true);
    addLog("⚡ Initiating A/B validation + model retrain pipeline…", "warn");
    try {
      const res = await triggerRetrain();
      addLog(`✓ ${res.status}: ${res.message}`, "success");
    } catch (e: any) {
      addLog(`✗ Retrain failed: ${e.message}`, "error");
    } finally {
      setRetraining(false);
    }
  };

  const handleUpdateForecast = async () => {
    setUpdating(true);
    addLog("⟳ Triggering 24h prediction regeneration…", "info");
    try {
      const res = await triggerForecastUpdate();
      addLog(`✓ ${res.message}`, "success");
      setTimeout(() => onForecastUpdated?.(), 3000);
    } catch (e: any) {
      addLog(`✗ Update failed: ${e.message}`, "error");
    } finally {
      setUpdating(false);
    }
  };

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          <motion.div
            key="overlay"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50"
            style={{ background: "rgba(0,0,0,0.6)" }}
            onClick={onClose}
          />

          <motion.div
            key="terminal"
            initial={{ opacity: 0, scale: 0.95, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 10 }}
            transition={{ type: "spring", damping: 25, stiffness: 250 }}
            className="fixed inset-0 m-auto z-50 rounded-xl overflow-hidden"
            style={{
              width: 560,
              maxWidth: "95vw",
              height: "auto",
              maxHeight: "85vh",
              background: "#040810",
              border: "1px solid rgba(0,229,255,0.15)",
              boxShadow: "0 0 60px rgba(0,100,200,0.12), 0 25px 50px rgba(0,0,0,0.5)",
              fontFamily: "'Share Tech Mono', monospace",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div
              className="flex items-center justify-between px-4 py-2.5 border-b"
              style={{ borderColor: "rgba(0,229,255,0.1)", background: "rgba(0,229,255,0.03)" }}
            >
              <div className="flex items-center gap-3">
                <div className="flex gap-1.5">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ background: "#ff5f57" }} />
                  <div className="w-2.5 h-2.5 rounded-full" style={{ background: "#ffbd2e" }} />
                  <div className="w-2.5 h-2.5 rounded-full" style={{ background: "#28c840" }} />
                </div>
                <span className="text-[11px] font-black tracking-[0.2em] uppercase" style={{ color: "#4fc3f7" }}>
                  AEGIS MLOps Terminal
                </span>
              </div>
              <button onClick={onClose} className="text-slate-500 hover:text-white text-lg transition-colors">
                ×
              </button>
            </div>

            <div className="p-4 overflow-y-auto" style={{ maxHeight: "calc(85vh - 44px)" }}>
              <div
                className="rounded-lg p-3 mb-4 grid grid-cols-2 gap-2 text-[10px]"
                style={{ background: "rgba(0,229,255,0.03)", border: "1px solid rgba(0,229,255,0.1)" }}
              >
                {[
                  { label: "LAST MODEL UPDATE",   value: fmt(metrics.last_model_update) },
                  { label: "FORECAST GENERATED",  value: fmt(metrics.prediction_generated_at) },
                  { label: "FORECAST WINDOW",      value: `${metrics.forecast_hours}h` },
                  { label: "WEATHER LIVE",         value: metrics.weather_live ? "✓ CONNECTED" : "✗ OFFLINE" },
                  { label: "REGIONS AT RISK",      value: String(metrics.total_regions_at_risk) },
                  { label: "LIVE ALARMS",          value: String(metrics.live_alarms_count) },
                ].map(({ label, value }) => (
                  <div key={label} className="flex flex-col gap-0.5">
                    <span style={{ color: "#475569" }}>{label}</span>
                    <span
                      style={{
                        color: value.includes("✗") ? "#ff1a3d" : value.includes("✓") ? "#4ade80" : "#94a3b8",
                        fontWeight: "bold",
                      }}
                    >
                      {value}
                    </span>
                  </div>
                ))}
              </div>

              <div className="flex gap-3 mb-4">
                <button
                  onClick={handleUpdateForecast}
                  disabled={updating}
                  className="flex-1 py-2.5 rounded-lg text-[10px] font-black tracking-[0.15em] uppercase transition-all duration-200 border"
                  style={{
                    borderColor: updating ? "rgba(0,229,255,0.2)" : "rgba(0,229,255,0.4)",
                    background: updating ? "rgba(0,229,255,0.03)" : "rgba(0,229,255,0.06)",
                    color: updating ? "#64748b" : "#00e5ff",
                    cursor: updating ? "not-allowed" : "pointer",
                  }}
                >
                  {updating ? "⟳ UPDATING…" : "⟳ REFRESH FORECAST"}
                </button>

                <button
                  onClick={handleRetrain}
                  disabled={retraining}
                  className="flex-1 py-2.5 rounded-lg text-[10px] font-black tracking-[0.15em] uppercase transition-all duration-200 border"
                  style={{
                    borderColor: retraining ? "rgba(255,26,61,0.2)" : "rgba(255,26,61,0.5)",
                    background: retraining ? "rgba(255,26,61,0.03)" : "rgba(255,26,61,0.08)",
                    color: retraining ? "#64748b" : "#ff1a3d",
                    cursor: retraining ? "not-allowed" : "pointer",
                    boxShadow: retraining ? "none" : "0 0 12px rgba(255,26,61,0.15)",
                  }}
                >
                  {retraining ? "⚡ PIPELINE RUNNING…" : "⚡ FORCE RETRAIN"}
                </button>
              </div>

              <div
                className="rounded-lg p-3 text-[10px] overflow-y-auto"
                style={{
                  background: "#020509",
                  border: "1px solid rgba(255,255,255,0.04)",
                  maxHeight: 180,
                  minHeight: 80,
                }}
              >
                {logs.map((line, i) => (
                  <div key={i} className="flex gap-2 mb-1">
                    <span style={{ color: "#334155", flexShrink: 0 }}>
                      {new Date(line.ts).toTimeString().slice(0, 8)}
                    </span>
                    <span style={{ color: TYPE_COLORS[line.type] }}>{line.msg}</span>
                  </div>
                ))}
                <div className="flex gap-2 animate-pulse">
                  <span style={{ color: "#334155" }}>
                    {new Date().toTimeString().slice(0, 8)}
                  </span>
                  <span style={{ color: "#4fc3f7" }}>█</span>
                </div>
              </div>

              <p
                className="text-[8px] text-center mt-3 opacity-40"
                style={{ color: "#64748b", fontFamily: "sans-serif" }}
              >
                Retrain runs as a background subprocess. Check /api/health for pipeline status.
              </p>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}