"use client";

interface ThreatTypesProps {
  missile: number;
  drone: number;
  artillery: number;
}

function TypeBar({ label, value, color, icon }: { label: string; value: number; color: string; icon: string }) {
  const pct = Math.round(value * 100);
  return (
    <div className="flex items-center gap-2 mb-2">
      <span className="text-sm w-4">{icon}</span>
      <span className="text-xs w-16" style={{ color: "#64748b" }}>{label}</span>
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

export default function ThreatTypes({ missile, drone, artillery }: ThreatTypesProps) {
  return (
    <div>
      <p className="text-xs mb-2 font-semibold tracking-wider uppercase" style={{ color: "#64748b" }}>
        Threat Types
      </p>
      <TypeBar label="Missile" value={missile} color="#ef4444" icon="🚀" />
      <TypeBar label="Drone" value={drone} color="#f59e0b" icon="🚁" />
      <TypeBar label="Artillery" value={artillery} color="#84cc16" icon="💥" />
    </div>
  );
}
