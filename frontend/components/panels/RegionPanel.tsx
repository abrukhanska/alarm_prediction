"use client";
import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { fetchPrediction, fetchWeather } from "@/lib/api";
import { PredictionResponse, WeatherResponse } from "@/lib/types";
import ThreatGauge from "./ThreatGauge";
import ProbabilityBars from "./ProbabilityBars";
import ThreatTypes from "./ThreatTypes";
import WeatherCard from "./WeatherCard";
import ForecastTimeline from "../timeline/ForecastTimeline";

export default function RegionPanel({ regionId, onClose }: { regionId: string | null, onClose: () => void }) {
  const [prediction, setPrediction] = useState<PredictionResponse | null>(null);
  const [weather, setWeather] = useState<WeatherResponse | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!regionId) return;
    setLoading(true);
    Promise.all([fetchPrediction(regionId), fetchWeather(regionId)])
      .then(([pred, wx]) => { setPrediction(pred); setWeather(wx); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [regionId]);

  const timelineData = prediction?.forecast
    ? Object.entries(prediction.forecast).map(([hour, isAlarm]) => ({
        hour,
        probability: isAlarm ? 0.95 : 0.05,
        missile: 0,
        drone: 0,
        artillery: 0
      }))
    : [];

  const currentHour = new Date().getHours();

  return (
    <AnimatePresence>
      {regionId && (
        <motion.div
          initial={{ x: "100%" }}
          animate={{ x: 0 }}
          exit={{ x: "100%" }}
          transition={{ type: "spring", damping: 25, stiffness: 200 }}
          className="fixed right-4 top-24 bottom-40 w-80 z-50 glass-panel rounded-2xl border border-cyan-500/20 bg-slate-950/60 backdrop-blur-xl overflow-y-auto custom-scrollbar shadow-2xl"
        >
          {loading ? (
            <div className="flex items-center justify-center h-full">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-cyan-500" />
            </div>
          ) : prediction ? (
            <div className="p-5 space-y-6">

              <div className="flex justify-between items-start">
                <h2 className="text-xl font-black text-white italic">
                  {regionId.includes("kyiv") ? "KYIV" : prediction.region_name.toUpperCase()}
                </h2>
                <button onClick={onClose} className="text-slate-500 hover:text-white text-xl">✕</button>
              </div>

              <ThreatGauge probability={prediction.probability_1h} label="Risk Level" />
              <ProbabilityBars p1h={prediction.probability_1h} p3h={prediction.probability_3h} p6h={prediction.probability_6h} p12h={prediction.probability_12h} />

              <ThreatTypes missile={prediction.threat_types.missile} drone={prediction.threat_types.drone} artillery={prediction.threat_types.artillery} />
              {timelineData.length > 0 && (
                <div className="h-40 w-full mt-4">
                   <p className="text-xs mb-2 font-semibold tracking-wider uppercase" style={{ color: "#64748b" }}>
                     24H Forecast
                   </p>
                   <ForecastTimeline hours={timelineData} currentHour={currentHour} />
                </div>
              )}
              <WeatherCard weather={weather} />

            </div>
          ) : null}
        </motion.div>
      )}
    </AnimatePresence>
  );
}