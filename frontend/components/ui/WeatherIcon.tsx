"use client";

interface WeatherIconProps {
  icon: string;
  size?: number;
  className?: string;
}

export default function WeatherIcon({ icon, size = 14, className = "" }: WeatherIconProps) {
  const s = size;
  const half = s / 2;

  switch (icon) {
    case "sun":
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <circle cx="8" cy="8" r="3.5" fill="#fbbf24" />
          {[0,45,90,135,180,225,270,315].map((deg) => {
            const rad = (deg * Math.PI) / 180;
            const x1 = 8 + 5 * Math.cos(rad);
            const y1 = 8 + 5 * Math.sin(rad);
            const x2 = 8 + 6.5 * Math.cos(rad);
            const y2 = 8 + 6.5 * Math.sin(rad);
            return <line key={deg} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#fbbf24" strokeWidth="1.5" strokeLinecap="round" />;
          })}
        </svg>
      );

    case "partly-cloudy":
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <circle cx="6" cy="7" r="2.5" fill="#fbbf24" opacity="0.9" />
          <ellipse cx="9" cy="10" rx="4" ry="2.5" fill="#94a3b8" />
          <ellipse cx="7" cy="10.5" rx="3" ry="2" fill="#94a3b8" />
          <ellipse cx="10" cy="9.5" rx="2.5" ry="2" fill="#cbd5e1" />
        </svg>
      );

    case "cloud":
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <ellipse cx="8" cy="9.5" rx="5.5" ry="3.5" fill="#64748b" />
          <ellipse cx="6" cy="9" rx="3.5" ry="3" fill="#64748b" />
          <ellipse cx="10" cy="8.5" rx="3" ry="3" fill="#94a3b8" />
        </svg>
      );

    case "drizzle":
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <ellipse cx="8" cy="7" rx="5" ry="3" fill="#64748b" />
          <ellipse cx="6" cy="6.5" rx="3" ry="2.5" fill="#64748b" />
          {[4,7,10].map((x) => (
            <line key={x} x1={x} y1="11" x2={x-1} y2="13.5" stroke="#7dd3fc" strokeWidth="1" strokeLinecap="round" />
          ))}
        </svg>
      );

    case "rain":
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <ellipse cx="8" cy="6" rx="5" ry="3" fill="#475569" />
          <ellipse cx="6" cy="5.5" rx="3" ry="2.5" fill="#475569" />
          {[3,6,9,12].map((x) => (
            <line key={x} x1={x} y1="10" x2={x-1.5} y2="14" stroke="#38bdf8" strokeWidth="1.2" strokeLinecap="round" />
          ))}
        </svg>
      );

    case "snow":
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <ellipse cx="8" cy="6" rx="5" ry="3" fill="#64748b" />
          {[3,6,9,12].map((x) => (
            <g key={x}>
              <circle cx={x} cy="12" r="1" fill="#e2e8f0" opacity="0.9" />
            </g>
          ))}
        </svg>
      );

    default:
      return (
        <svg width={s} height={s} viewBox="0 0 16 16" className={className} fill="none">
          <circle cx="8" cy="8" r="5" fill="#334155" />
        </svg>
      );
  }
}