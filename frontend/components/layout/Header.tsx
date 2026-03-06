"use client";
import { useEffect, useState } from "react";
import AnimatedNumber from "@/components/ui/AnimatedNumber";
import PulsingDot from "@/components/ui/PulsingDot";
import { StatsResponse } from "@/lib/types";

interface HeaderProps {
    stats: StatsResponse | null;
}

export default function Header({ stats }: HeaderProps) {
    const [clock, setClock] = useState("");

    useEffect(() => {
        const tick = () => {
            const now = new Date();
            setClock(
                now.toLocaleTimeString("en-GB", {
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    timeZone: "UTC",
                }) + " UTC"
            );
        };
        tick();
        const id = setInterval(tick, 1000);
        return () => clearInterval(id);
    }, []);

    return (
        <header
            className="flex items-center justify-between px-6 py-3 border-b glass-panel z-20 relative"
            style={{ borderColor: "rgba(30, 58, 95, 0.6)" }}
        >
            <div className="flex items-center gap-3">
                <span className="text-2xl drop-shadow-[0_0_8px_rgba(6,182,212,0.8)]">🛡️</span>
                <div>
          <span className="font-bold text-xl tracking-wide drop-shadow-[0_0_5px_rgba(6,182,212,0.5)]" style={{ color: "#06b6d4" }}>
            AEGIS
          </span>
                    <span className="text-[#64748b] text-sm ml-2 hidden sm:inline">
            Air Event Guardian &amp; Intelligence System
          </span>
                </div>
            </div>

            <div className="flex items-center gap-8">
                {stats && (
                    <>
                        <div className="flex items-center gap-2">
                            <PulsingDot color="#ef4444" size={8} />
                            <span className="text-[#64748b] text-sm">Active Threats:</span>
                            <span className="font-bold text-[#ef4444]">
                <AnimatedNumber value={stats.active_alarms_count} /> / {stats.total_regions}
              </span>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-[#64748b] text-sm">Avg Level:</span>
                            <span className="font-bold text-[#f59e0b]">
                                <AnimatedNumber value={stats.avg_threat_level} decimals={1} suffix=" / 10" />
              </span>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-[#64748b] text-sm">Today:</span>
                            <span className="font-bold text-[#e2e8f0]">
                <AnimatedNumber value={stats.total_alarms_today} /> alarms
              </span>
                        </div>
                    </>
                )}
            </div>

            <div
                className="font-mono text-lg font-bold tracking-widest drop-shadow-[0_0_5px_rgba(6,182,212,0.5)]"
                style={{ color: "#06b6d4" }}
            >
                {clock}
            </div>
        </header>
    );
}