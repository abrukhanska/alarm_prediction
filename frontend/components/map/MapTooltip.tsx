"use client";
import { motion } from "framer-motion";
import { threatColor } from "@/lib/colors";
import { RegionAlarm } from "@/lib/types";

interface TooltipProps {
  x: number;
  y: number;
  region: RegionAlarm | undefined;
  shapeName: string;
}

export default function MapTooltip({ x, y, region, shapeName }: TooltipProps) {
  const name = region?.name ?? shapeName;
  const level = region?.threat_level ?? "unknown";
  const color = threatColor(level);

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.9 }}
      transition={{ duration: 0.15 }}
      className="pointer-events-none fixed z-50 rounded-lg px-3 py-2 text-sm shadow-xl"
      style={{
        left: x + 12,
        top: y - 10,
        background: "#111827",
        border: `1px solid ${color}60`,
        minWidth: 140,
      }}
    >
      <div className="font-semibold" style={{ color: "#e2e8f0" }}>{name}</div>
      <div className="flex items-center gap-1 mt-1">
        <span
          className="inline-block w-2 h-2 rounded-full"
          style={{ backgroundColor: color }}
        />
        <span className="capitalize" style={{ color }}>{level}</span>
        {region?.active && (
          <span className="ml-1 text-xs" style={{ color: "#ef4444" }}>● ACTIVE</span>
        )}
      </div>
      {region?.type && (
        <div className="text-xs mt-0.5" style={{ color: "#64748b" }}>
          Type: {region.type}
        </div>
      )}
    </motion.div>
  );
}
