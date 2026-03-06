"use client";
import { probToColor } from "@/lib/colors";

interface ProbabilityBarsProps {
  p1h: number;
  p3h: number;
  p6h: number;
  p12h: number;
}

function Bar({ label, value }: { label: string; value: number }) {
  const pct = Math.round(value * 100);
  const color = probToColor(value);
  return (
    <div className="flex items-center gap-2 mb-2">
      <span className="text-xs w-8 text-right" style={{ color: "#64748b" }}>{label}</span>
      <div className="flex-1 rounded-full h-2" style={{ background: "#1e3a5f" }}>
        <div
          className="h-2 rounded-full transition-all duration-500"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
      <span className="text-xs w-8" style={{ color }}>{pct}%</span>
    </div>
  );
}

export default function ProbabilityBars({ p1h, p3h, p6h, p12h }: ProbabilityBarsProps) {
  return (
    <div>
      <p className="text-xs mb-2 font-semibold tracking-wider uppercase" style={{ color: "#64748b" }}>
        Forecast Probability
      </p>
      <Bar label="1h" value={p1h} />
      <Bar label="3h" value={p3h} />
      <Bar label="6h" value={p6h} />
      <Bar label="12h" value={p12h} />
    </div>
  );
}
