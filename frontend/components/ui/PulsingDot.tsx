"use client";
interface PulsingDotProps {
  color?: string;
  size?: number;
}

export default function PulsingDot({ color = "#dc2626", size = 10 }: PulsingDotProps) {
  return (
    <span className="relative inline-flex" style={{ width: size, height: size }}>
      <span
        className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75"
        style={{ backgroundColor: color }}
      />
      <span
        className="relative inline-flex rounded-full"
        style={{ width: size, height: size, backgroundColor: color }}
      />
    </span>
  );
}
