"use client";
import { useState } from "react";
import { threatColor } from "@/lib/colors";
import { RegionAlarm } from "@/lib/types";

interface RegionPathProps {
  path: string;
  regionId: string;
  alarm: RegionAlarm | undefined;
  isSelected: boolean;
  onHover: (id: string | null, x: number, y: number) => void;
  onClick: (id: string) => void;
}

export default function RegionPath({
  path,
  regionId,
  alarm,
  isSelected,
  onHover,
  onClick,
}: RegionPathProps) {
  const [hovered, setHovered] = useState(false);
  const level = alarm?.threat_level ?? "safe";
  const color = threatColor(level);
  const isActive = alarm?.active ?? false;

  const fill = isSelected
    ? color
    : hovered
    ? color + "cc"
    : color + "66";

  return (
    <path
      d={path}
      fill={fill}
      stroke={isActive ? "#dc2626" : isSelected ? "#06b6d4" : "#1e3a5f"}
      strokeWidth={isActive ? 1.5 : isSelected ? 2 : 0.5}
      className={isActive ? "pulse-alarm" : ""}
      style={{
        cursor: "pointer",
        transition: "fill 0.5s ease, stroke 0.3s ease",
      }}
      onMouseEnter={(e) => {
        setHovered(true);
        onHover(regionId, e.clientX, e.clientY);
      }}
      onMouseMove={(e) => {
        onHover(regionId, e.clientX, e.clientY);
      }}
      onMouseLeave={() => {
        setHovered(false);
        onHover(null, 0, 0);
      }}
      onClick={() => onClick(regionId)}
    />
  );
}
