"use client";

interface ThreatGaugeProps {
  probability: number; // 0-1
  label?: string;
}

export default function ThreatGauge({ probability, label }: ThreatGaugeProps) {
  const pct = Math.round(probability * 100);
  const radius = 54;
  const circumference = 2 * Math.PI * radius;
  const arcLength = Math.PI * radius;
  const fill = (pct / 100) * arcLength;

  const color = pct >= 80 ? "#dc2626" : pct >= 60 ? "#ef4444" : pct >= 40 ? "#f59e0b" : pct >= 20 ? "#84cc16" : "#10b981";

  return (
    <div className="flex flex-col items-center">
      <div className="relative" style={{ width: 130, height: 70 }}>
        <svg width="130" height="80" viewBox="0 0 130 80">
          <path
            d="M 10 70 A 55 55 0 0 1 120 70"
            fill="none"
            stroke="#1e3a5f"
            strokeWidth="12"
            strokeLinecap="round"
          />
          <path
            d="M 10 70 A 55 55 0 0 1 120 70"
            fill="none"
            stroke={color}
            strokeWidth="12"
            strokeLinecap="round"
            strokeDasharray={`${(pct / 100) * 172} 172`}
            style={{ transition: "stroke-dasharray 0.5s ease, stroke 0.5s ease" }}
          />
          <text x="65" y="68" textAnchor="middle" fontSize="22" fontWeight="bold" fill={color}>
            {pct}%
          </text>
        </svg>
      </div>
      {label && (
        <span className="text-xs mt-1" style={{ color: "#64748b" }}>{label}</span>
      )}
    </div>
  );
}
