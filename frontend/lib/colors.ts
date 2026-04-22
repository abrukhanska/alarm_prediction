export const THREAT_COLORS: Record<string, string> = {
  critical: "#ff1a3d",
  high:     "#ff6b00",
  medium:   "#ffc800",
  low:      "#66dd00",
  safe:     "#004d2e",
  default:  "#071325",
};

export function probToColor(prob: number): string {
  if (prob >= 80) return THREAT_COLORS.critical;
  if (prob >= 60) return THREAT_COLORS.high;
  if (prob >= 40) return THREAT_COLORS.medium;
  if (prob >= 20) return THREAT_COLORS.low;
  return THREAT_COLORS.safe;
}

export function probToLevel(prob: number): string {
  if (prob >= 80) return "critical";
  if (prob >= 60) return "high";
  if (prob >= 40) return "medium";
  if (prob >= 20) return "low";
  return "safe";
}

export function riskIndexToColor(idx: number): string {
  return probToColor(idx);
}

export function hexToRgba(hex: string, alpha: number): string {
  const c = hex.replace("#", "");
  const r = parseInt(c.slice(0, 2), 16);
  const g = parseInt(c.slice(2, 4), 16);
  const b = parseInt(c.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

export const RISK_LEVEL_LABELS: Record<string, string> = {
  RED:    "CRITICAL",
  YELLOW: "ELEVATED",
  GREEN:  "NORMAL",
  critical: "CRITICAL",
  high:     "HIGH",
  medium:   "ELEVATED",
  low:      "LOW",
  safe:     "NORMAL",
};