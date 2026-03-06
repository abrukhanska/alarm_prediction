"use client";
import { useState } from "react";
import { ComposableMap, Geographies, Geography } from "react-simple-maps";
import { AnimatePresence } from "framer-motion";
import { SHAPE_NAME_TO_ID } from "@/lib/regions";
import { RegionAlarm } from "@/lib/types";
import { threatColor } from "@/lib/colors";
import MapTooltip from "./MapTooltip";

const GEO_URL = "/geo/ukraine-adm1.json";

interface UkraineMapProps {
    alarms: RegionAlarm[];
    selectedRegion: string | null;
    onSelectRegion: (id: string) => void;
}

const THREAT_LEVELS = ["safe", "low", "medium", "high", "critical"] as const;

export default function UkraineMap({ alarms, selectedRegion, onSelectRegion }: UkraineMapProps) {
    const [tooltip, setTooltip] = useState<{
        id: string;
        name: string;
        x: number;
        y: number;
    } | null>(null);

    const alarmMap = new Map(alarms.map((a) => [a.id, a]));

    const handleHover = (id: string | null, name: string, x: number, y: number) => {
        if (id) setTooltip({ id, name, x, y });
        else setTooltip(null);
    };

    const tooltipAlarm = tooltip ? alarmMap.get(tooltip.id) : undefined;

    return (
        <div className="relative w-full h-full flex items-center justify-center p-4">
            <ComposableMap
                projection="geoMercator"
                projectionConfig={{
                    center: [31.5, 48.5],
                    scale: 2300,
                }}
                className="w-full h-full max-h-[90vh] object-contain drop-shadow-2xl"
                preserveAspectRatio="xMidYMid meet"
            >
                <Geographies geography={GEO_URL}>
                    {({ geographies }) =>
                        geographies.map((geo) => {
                            const shapeName = geo.properties.shapeName as string;
                            const regionId = SHAPE_NAME_TO_ID[shapeName] ?? shapeName;
                            const alarm = alarmMap.get(regionId);

                            const level = alarm?.threat_level ?? "safe";
                            const color = threatColor(level);
                            const isActive = alarm?.active ?? false;
                            const isSelected = selectedRegion === regionId;

                            const fillOpacity = isSelected ? 0.9 : isActive ? 0.6 : 0.3;

                            return (
                                <Geography
                                    key={geo.rsmKey}
                                    geography={geo}
                                    fill={color}
                                    fillOpacity={fillOpacity}
                                    stroke={isActive ? "#ef4444" : isSelected ? "#06b6d4" : "#1e3a5f"}
                                    strokeWidth={isActive ? 1.5 : isSelected ? 2 : 0.5}
                                    className={isActive ? "pulse-alarm" : ""}
                                    style={{
                                        default: {
                                            outline: "none",
                                            transition: "fill 0.4s ease, stroke 0.3s ease, fill-opacity 0.4s ease",
                                        },
                                        hover: {
                                            outline: "none",
                                            fill: color,
                                            fillOpacity: 0.8,
                                            cursor: "pointer",
                                            stroke: "#06b6d4",
                                            strokeWidth: 1.5,
                                        },
                                        pressed: { outline: "none" },
                                    }}
                                    onMouseEnter={(e: React.MouseEvent) => {
                                        handleHover(regionId, shapeName, e.clientX, e.clientY);
                                    }}
                                    onMouseMove={(e: React.MouseEvent) => {
                                        handleHover(regionId, shapeName, e.clientX, e.clientY);
                                    }}
                                    onMouseLeave={() => handleHover(null, "", 0, 0)}
                                    onClick={() => onSelectRegion(regionId)}
                                />
                            );
                        })
                    }
                </Geographies>
            </ComposableMap>

            <AnimatePresence>
                {tooltip && (
                    <MapTooltip
                        key={tooltip.id}
                        x={tooltip.x}
                        y={tooltip.y}
                        region={tooltipAlarm}
                        shapeName={tooltip.name}
                    />
                )}
            </AnimatePresence>

            <div
                className="absolute bottom-6 right-6 glass-panel p-4 rounded-xl text-xs"
                style={{ pointerEvents: "none" }}
            >
                <p className="font-semibold mb-3 uppercase tracking-wider text-slate-400">
                    Threat Level
                </p>
                {THREAT_LEVELS.map((lvl) => (
                    <div key={lvl} className="flex items-center gap-3 mb-1.5">
            <span
                className="inline-block w-3.5 h-3.5 rounded-sm shadow-md"
                style={{ backgroundColor: threatColor(lvl), border: `1px solid ${threatColor(lvl)}cc` }}
            />
                        <span className="capitalize text-slate-200" style={{ color: lvl === 'safe' ? '#64748b' : '#e2e8f0' }}>
              {lvl}
            </span>
                    </div>
                ))}
            </div>
        </div>
    );
}