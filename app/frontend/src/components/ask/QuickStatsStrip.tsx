"use client";

import { useEffect, useState } from "react";
import { Activity, DollarSign, TrendingUp } from "lucide-react";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

interface QuickStats {
  today_revenue: number | null;
  mtd_events: number | null;
  mtd_top_brand_name: string | null;
  mtd_top_brand_revenue: number | null;
  cached_at: string;
  source_status: "ok" | "partial" | "unavailable";
}

function formatCurrency(v: number | null): string {
  if (v === null || v === undefined) return "-";
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return v.toLocaleString();
}

function formatInt(v: number | null): string {
  if (v === null || v === undefined) return "-";
  return v.toLocaleString();
}

const SKELETON = (
  <div className="h-3 w-20 animate-pulse rounded bg-border" aria-hidden="true" />
);

export function QuickStatsStrip() {
  const [data, setData] = useState<QuickStats | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api
      .get<QuickStats>("/quickstats")
      .then((value) => {
        if (!cancelled) setData(value);
      })
      .catch(() => {
        /* QuickStats is nice-to-have; never block the Ask flow on failure. */
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (loaded && !data) {
    return (
      <div className="rounded-lg border border-border bg-surface p-3 text-[11px] text-text-secondary shadow-card">
        Quick stats unavailable. Trino may be down.
      </div>
    );
  }

  const unavailable = data?.source_status === "unavailable";

  return (
    <div
      className={cn(
        "grid grid-cols-1 gap-3 sm:grid-cols-3",
      )}
    >
      <Stat
        icon={DollarSign}
        label="Today revenue"
        value={loaded ? formatCurrency(data?.today_revenue ?? null) : null}
        muted={unavailable}
      />
      <Stat
        icon={Activity}
        label="MTD events"
        value={loaded ? formatInt(data?.mtd_events ?? null) : null}
        muted={unavailable}
      />
      <Stat
        icon={TrendingUp}
        label="Top brand MTD"
        value={
          loaded
            ? data?.mtd_top_brand_name
              ? `${data.mtd_top_brand_name}`
              : "-"
            : null
        }
        sub={
          loaded && data?.mtd_top_brand_revenue != null
            ? formatCurrency(data.mtd_top_brand_revenue)
            : undefined
        }
        muted={unavailable}
      />
    </div>
  );
}

function Stat({
  icon: Icon,
  label,
  value,
  sub,
  muted,
}: {
  icon: typeof Activity;
  label: string;
  value: string | null;
  sub?: string;
  muted?: boolean;
}) {
  return (
    <div
      className={cn(
        "flex items-center gap-3 rounded-lg border border-border bg-surface p-3 shadow-card",
        muted && "opacity-60",
      )}
    >
      <span className="inline-flex h-8 w-8 items-center justify-center rounded-md bg-accent/10 text-accent">
        <Icon className="h-4 w-4" aria-hidden="true" />
      </span>
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-wide text-text-secondary">{label}</p>
        <p className="truncate text-sm font-semibold text-text-primary">
          {value === null ? SKELETON : value}
        </p>
        {sub && <p className="text-[11px] text-text-secondary">{sub}</p>}
      </div>
    </div>
  );
}
