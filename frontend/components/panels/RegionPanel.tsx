"use client";
import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { fetchPrediction, fetchWeather } from "@/lib/api";
import { threatColor } from "@/lib/colors";
import { PredictionResponse, WeatherResponse } from "@/lib/types";
import ThreatGauge from "./ThreatGauge";
import ProbabilityBars from "./ProbabilityBars";
import ThreatTypes from "./ThreatTypes";
import WeatherCard from "./WeatherCard";

interface RegionPanelProps {
  regionId: string | null;
  onClose: () => void;
}

export default function RegionPanel({ regionId, onClose }: RegionPanelProps) {
  const [prediction, setPrediction] = useState<PredictionResponse | null>(null);
  const [weather, setWeather] = useState<WeatherResponse | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!regionId) return;
    setLoading(true);
    Promise.all([
      fetchPrediction(regionId),
      fetchWeather(regionId),
    ])
      .then(([pred, wx]) => {
        setPrediction(pred);
        setWeather(wx);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [regionId]);

  return (
    <AnimatePresence>
      {regionId && (
          <motion.div
              key="panel"
              initial={{ x: "100%", opacity: 0 }}
              animate={{ x: 0, opacity: 1 }}
              exit={{ x: "100%", opacity: 0 }}
              transition={{ type: "spring", stiffness: 300, damping: 30 }}
              className="w-80 flex-shrink-0 overflow-y-auto rounded-l-xl glass-panel z-20 relative"
              style={{ borderLeft: "1px solid rgba(6, 182, 212, 0.3)", borderRight: "none" }}
          >
          {loading ? (
            <div className="flex items-center justify-center h-32">
              <div className="animate-spin w-8 h-8 rounded-full border-2 border-[#06b6d4] border-t-transparent" />
            </div>
          ) : prediction ? (
            <div className="p-4 space-y-4">
              <div className="flex items-start justify-between">
                <div>
                  <h2 className="font-bold text-lg leading-tight" style={{ color: "#e2e8f0" }}>
                    {prediction.region_name}
                  </h2>
                  <span
                    className="inline-block mt-1 px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider"
                    style={{
                      background: threatColor(prediction.threat_level) + "30",
                      color: threatColor(prediction.threat_level),
                      border: `1px solid ${threatColor(prediction.threat_level)}60`,
                    }}
                  >
                    {prediction.threat_level}
                  </span>
                </div>
                <button
                  onClick={onClose}
                  className="text-[#64748b] hover:text-[#e2e8f0] text-xl leading-none"
                >
                  ✕
                </button>
              </div>

              <div className="flex justify-center py-2">
                <ThreatGauge
                  probability={prediction.probability_1h}
                  label="1-hour probability"
                />
              </div>

              <ProbabilityBars
                p1h={prediction.probability_1h}
                p3h={prediction.probability_3h}
                p6h={prediction.probability_6h}
                p12h={prediction.probability_12h}
              />

              <ThreatTypes
                missile={prediction.threat_types.missile}
                drone={prediction.threat_types.drone}
                artillery={prediction.threat_types.artillery}
              />

              <WeatherCard weather={weather} />

              <p className="text-xs text-center" style={{ color: "#64748b" }}>
                Updated: {new Date(prediction.updated_at).toLocaleTimeString()}
              </p>
            </div>
          ) : null}
        </motion.div>
      )}
    </AnimatePresence>
  );
}
