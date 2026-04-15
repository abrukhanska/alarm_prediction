"use client";
import { useState } from "react";
import { ComposableMap, Geographies, Geography, Marker } from "react-simple-maps";
import { AnimatePresence } from "framer-motion";
import { SHAPE_NAME_TO_ID } from "@/lib/regions";
import { RegionAlarm } from "@/lib/types";
import { threatColor } from "@/lib/colors";
import MapTooltip from "./MapTooltip";

const GEO_URL = "/geo/ukraine-adm1.json";

const REGION_LABELS: Record<string, { coords: [number, number], name: string }> = {
  "kyiv_oblast": { coords: [30.52, 50.45], name: "KYIV" }, // Ставимо напис KYIV замість області
  "lviv": { coords: [24.03, 49.84], name: "Lviv" },
  "odesa": { coords: [30.72, 46.48], name: "Odesa" },
  "kharkiv": { coords: [36.23, 50.00], name: "Kharkiv" },
  "dnipropetrovsk": { coords: [35.04, 48.46], name: "Dnipro" },
  "zaporizhzhia": { coords: [35.13, 47.83], name: "Zaporizhzhia" },
  "donetsk": { coords: [37.80, 48.01], name: "Donetsk" },
  "luhansk": { coords: [39.30, 48.57], name: "Luhansk" },
  "kherson": { coords: [32.61, 46.63], name: "Kherson" },
  "crimea": { coords: [34.10, 44.95], name: "Crimea" },
  "mykolaiv": { coords: [31.99, 46.97], name: "Mykolaiv" },
  "vinnytsia": { coords: [28.46, 49.23], name: "Vinnytsia" },
  "chernihiv": { coords: [31.28, 51.50], name: "Chernihiv" },
  "poltava": { coords: [34.55, 49.58], name: "Poltava" },
  "sumy": { coords: [34.79, 50.90], name: "Sumy" },
  "cherkasy": { coords: [32.05, 49.44], name: "Cherkasy" },
  "khmelnytskyi": { coords: [26.98, 49.42], name: "Khmelnytskyi" },
  "zhytomyr": { coords: [28.65, 50.25], name: "Zhytomyr" },
  "chernivtsi": { coords: [25.93, 48.29], name: "Chernivtsi" },
  "rivne": { coords: [26.25, 50.61], name: "Rivne" },
  "ivano_frankivsk": { coords: [24.71, 48.92], name: "Ivano-Frankivsk" },
  "ternopil": { coords: [25.59, 49.55], name: "Ternopil" },
  "volyn": { coords: [25.32, 50.74], name: "Lutsk" },
  "zakarpattia": { coords: [22.28, 48.62], name: "Uzhhorod" },
  "kirovohrad": { coords: [32.26, 48.50], name: "Kropyvnytskyi" },
};

interface UkraineMapProps {
  alarms: RegionAlarm[];
  selectedRegion: string | null;
  onSelectRegion: (id: string) => void;
}

export default function UkraineMap({ alarms, selectedRegion, onSelectRegion }: UkraineMapProps) {
  const [tooltip, setTooltip] = useState<{ id: string; name: string; x: number; y: number } | null>(null);
  const alarmMap = new Map(alarms.map((a) => [a.id, a]));

  const handleHover = (id: string | null, name: string, x: number, y: number) => {
    if (id) setTooltip({ id, name, x, y });
    else setTooltip(null);
  };

  return (
    <div className="relative w-full h-full flex items-center justify-center p-4">
      <ComposableMap
        projection="geoMercator"
        projectionConfig={{ center: [31.5, 48.5], scale: 2300 }}
        className="w-full h-full max-h-[90vh] object-contain drop-shadow-2xl"
      >
        <Geographies geography={GEO_URL}>
          {({ geographies }) => (
            <>
              {geographies.map((geo) => {
                const shapeName = geo.properties.shapeName as string;
                const regionId = SHAPE_NAME_TO_ID[shapeName] ?? shapeName;
                const alarm = alarmMap.get(regionId);
                const color = threatColor(alarm?.threat_level ?? "safe");
                const isActive = alarm?.active ?? false;
                const isSelected = selectedRegion === regionId;

                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={color}
                    fillOpacity={isSelected ? 0.9 : isActive ? 0.6 : 0.3}
                    stroke={isActive ? "#ef4444" : isSelected ? "#06b6d4" : "#1e3a5f"}
                    strokeWidth={isActive ? 1.5 : isSelected ? 2 : 0.5}
                    className={isActive ? "pulse-alarm" : ""}
                    style={{
                      default: { outline: "none", transition: "all 0.4s ease" },
                      hover: { outline: "none", fillOpacity: 0.8, cursor: "pointer", stroke: "#06b6d4" },
                      pressed: { outline: "none" },
                    }}
                    onMouseEnter={(e) => handleHover(regionId, shapeName, e.clientX, e.clientY)}
                    onMouseMove={(e) => handleHover(regionId, shapeName, e.clientX, e.clientY)}
                    onMouseLeave={() => handleHover(null, "", 0, 0)}
                    onClick={() => onSelectRegion(regionId)}
                  />
                );
              })}

              {geographies.map((geo) => {
                const shapeName = geo.properties.shapeName;
                const regionId = SHAPE_NAME_TO_ID[shapeName] ?? shapeName;
                const regionData = REGION_LABELS[regionId];
                const alarm = alarmMap.get(regionId);
                if (!regionData) return null;

                return (
                  <Marker key={`marker-${regionId}`} coordinates={regionData.coords}>
                    {alarm?.active && (
                      <g>
                        <circle r="6" className="radar-circle" />
                        <circle r="6" className="radar-circle" style={{ animationDelay: '1s' }} />
                      </g>
                    )}
                    <text
                      textAnchor="middle"
                      y={2}
                      className="region-label"
                      style={{ 
                        fontSize: regionId === "kyiv_oblast" ? "7px" : "6px", 
                        fontWeight: "800", 
                        fill: "#fff", 
                        pointerEvents: "none"
                      }}
                    >
                      {regionData.name}
                    </text>
                  </Marker>
                );
              })}
            </>
          )}
        </Geographies>
      </ComposableMap>

      <AnimatePresence>
        {tooltip && (
          <MapTooltip x={tooltip.x} y={tooltip.y} region={alarmMap.get(tooltip.id)} shapeName={tooltip.name} />
        )}
      </AnimatePresence>
    </div>
  );
}