export const THREAT_COLORS: Record<string, string> = {
  critical: "#ff1e1e",
  high:     "#f97316",
  medium:   "#eab308",
  low:      "#84cc16",
  safe:     "#064e3b",
  default:  "#1e3a5f",
};

export function threatColor(level: string): string {
  return THREAT_COLORS[level.toLowerCase()] ?? THREAT_COLORS.default;
}

export function hexToRgba(hex: string, alpha: number): string {
  const clean = hex.replace("#", "");
  const r = parseInt(clean.slice(0, 2), 16);
  const g = parseInt(clean.slice(2, 4), 16);
  const b = parseInt(clean.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

export function probToColor(prob: number): string {
  if (prob >= 0.80) return THREAT_COLORS.critical;
  if (prob >= 0.60) return THREAT_COLORS.high;
  if (prob >= 0.40) return THREAT_COLORS.medium;
  if (prob >= 0.20) return THREAT_COLORS.low;
  return THREAT_COLORS.safe;
}

export function probToLevel(prob: number): string {
  if (prob >= 0.80) return "critical";
  if (prob >= 0.60) return "high";
  if (prob >= 0.40) return "medium";
  if (prob >= 0.20) return "low";
  return "safe";
}