"use client";
import { useEffect, useRef } from "react";
import { riskIndexToColor, hexToRgba, probToLevel } from "@/lib/colors";

interface NationalRiskGaugeProps {
  value: number;
  liveAlarms: number;
  regionsAtRisk: number;
}

const DEFCON_LABELS: Record<string, string> = {
  safe:     "DEFCON 5 — PEACETIME",
  low:      "DEFCON 4 — ELEVATED",
  medium:   "DEFCON 3 — ROUND HOUSE",
  high:     "DEFCON 2 — FAST PACE",
  critical: "DEFCON 1 — COCKED PISTOL",
};

export default function NationalRiskGauge({ value, liveAlarms, regionsAtRisk }: NationalRiskGaugeProps) {
  const canvasRef   = useRef<HTMLCanvasElement>(null);
  const animRef     = useRef<number | null>(null);
  const displayRef  = useRef<number>(value);

  const color   = riskIndexToColor(value);
  const level   = probToLevel(value);
  const defcon  = DEFCON_LABELS[level];
  const isCritical = value >= 70;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const W  = canvas.width;
    const H  = canvas.height;
    const cx = W / 2;
    const cy = H * 0.75;
    const R  = W * 0.38;

    function draw(current: number) {
      if (!ctx) return;
      ctx.clearRect(0, 0, W, H);

      // background arc track
      ctx.beginPath();
      ctx.arc(cx, cy, R, Math.PI, 2 * Math.PI);
      ctx.strokeStyle = "rgba(255,255,255,0.04)";
      ctx.lineWidth   = 14;
      ctx.stroke();

      const zones = [
        { from: Math.PI,       to: Math.PI * 1.2, c: "#004d2e" },
        { from: Math.PI * 1.2, to: Math.PI * 1.4, c: "#1a6600" },
        { from: Math.PI * 1.4, to: Math.PI * 1.6, c: "#665500" },
        { from: Math.PI * 1.6, to: Math.PI * 1.8, c: "#7a3000" },
        { from: Math.PI * 1.8, to: Math.PI * 2,   c: "#660011" },
      ];
      zones.forEach(({ from, to, c }) => {
        ctx.beginPath();
        ctx.arc(cx, cy, R, from, to);
        ctx.strokeStyle = c;
        ctx.lineWidth   = 10;
        ctx.stroke();
      });

      const fillAngle = Math.PI + (current / 100) * Math.PI;
      const fillColor = riskIndexToColor(Math.round(current));
      ctx.beginPath();
      ctx.arc(cx, cy, R, Math.PI, fillAngle);
      ctx.strokeStyle  = fillColor;
      ctx.lineWidth    = 14;
      ctx.lineCap      = "round";
      ctx.shadowColor  = fillColor;
      ctx.shadowBlur   = 20;
      ctx.stroke();
      ctx.shadowBlur   = 0;

      const needleAngle = Math.PI + (current / 100) * Math.PI;
      const nx = cx + R * Math.cos(needleAngle);
      const ny = cy + R * Math.sin(needleAngle);
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.lineTo(nx, ny);
      ctx.strokeStyle  = "#ffffff";
      ctx.lineWidth    = 2;
      ctx.lineCap      = "round";
      ctx.shadowColor  = "#ffffff";
      ctx.shadowBlur   = 8;
      ctx.stroke();
      ctx.shadowBlur   = 0;

      ctx.beginPath();
      ctx.arc(cx, cy, 5, 0, 2 * Math.PI);
      ctx.fillStyle = "#ffffff";
      ctx.fill();
    }

    function animate() {
      const current = displayRef.current;
      const target  = value;
      const diff    = target - current;

      if (Math.abs(diff) < 0.15) {
        // close enough — snap to target and stop
        displayRef.current = target;
        draw(target);
        return;
      }

      displayRef.current = current + diff * 0.08;
      draw(displayRef.current);
      animRef.current = requestAnimationFrame(animate);
    }

    if (animRef.current !== null) {
      cancelAnimationFrame(animRef.current);
    }
    animRef.current = requestAnimationFrame(animate);

    return () => {
      if (animRef.current !== null) cancelAnimationFrame(animRef.current);
    };
  }, [value]);

  return (
    <div className="relative flex flex-col items-center select-none">
      <canvas
        ref={canvasRef}
        width={200}
        height={130}
        className="w-full max-w-[200px]"
      />

      <div
        className="font-mono text-xl font-black tracking-wider mt-1"
        style={{ color, textShadow: `0 0 20px ${color}` }}
      >
        {value}
        <span className="text-sm ml-0.5 opacity-70">%</span>
      </div>

      <div
        className={`text-[9px] font-black tracking-[0.2em] uppercase mt-2 text-center px-2 py-0.5 rounded border ${
          isCritical ? "animate-pulse" : ""
        }`}
        style={{
          color,
          borderColor: hexToRgba(color, 0.4),
          background:  hexToRgba(color, 0.08),
          fontFamily:  "'Share Tech Mono', monospace",
        }}
      >
        {defcon}
      </div>

      <div className="flex gap-4 mt-2 text-[9px]" style={{ fontFamily: "'Share Tech Mono', monospace" }}>
        <span style={{ color: "#4fc3f7" }}>
          <span className="opacity-50 mr-1">LIVE</span>
          <span className="font-bold">{liveAlarms}</span>
        </span>
        <span className="opacity-20">|</span>
        <span style={{ color: "#f97316" }}>
          <span className="opacity-50 mr-1">AT RISK</span>
          <span className="font-bold">{regionsAtRisk}</span>
        </span>
      </div>
    </div>
  );
}