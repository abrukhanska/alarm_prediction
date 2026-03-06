import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: "#0a0f1a",
        surface: "#111827",
        card: "#1a2332",
        border: "#1e3a5f",
        primary: "#e2e8f0",
        secondary: "#64748b",
        accent: "#06b6d4",
        safe: "#10b981",
        low: "#84cc16",
        medium: "#f59e0b",
        high: "#ef4444",
        critical: "#dc2626",
        mapDefault: "#1e3a5f",
      },
      fontFamily: {
        sans: ["Inter", "sans-serif"],
      },
      animation: {
        pulse: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        glow: "glow 2s ease-in-out infinite alternate",
      },
      keyframes: {
        glow: {
          "0%": { boxShadow: "0 0 5px #dc2626, 0 0 10px #dc2626" },
          "100%": { boxShadow: "0 0 20px #dc2626, 0 0 40px #dc2626" },
        },
      },
    },
  },
  plugins: [],
};

export default config;
