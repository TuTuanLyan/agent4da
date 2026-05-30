"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  BadgeDollarSign,
  BarChart3,
  Layers,
  Package,
  Percent,
  RotateCcw,
  ShoppingCart,
  TrendingUp,
} from "lucide-react";
import { ChartPanel } from "@/components/ask/ChartPanel";
import { TablePanel } from "@/components/ask/TablePanel";
import { api, ApiError } from "@/lib/api";
import { cn } from "@/lib/utils";

// --- response shapes (mirror app/backend/metrics/schemas.py) ----------------

interface Overview {
  event_date: string | null;
  today_revenue: number | null;
  today_events: number | null;
  today_purchases: number | null;
  today_conversion_rate: number | null;
  mtd_events: number | null;
  mtd_revenue: number | null;
  top_brand_mtd: string | null;
  top_brand_mtd_revenue: number | null;
}

interface RevenuePoint {
  event_date: string;
  total_revenue: number;
  total_events: number;
  total_purchases: number;
  conversion_rate: number;
  cart_to_purchase_rate: number;
}

interface BrandRow {
  brand: string;
  revenue: number;
  views: number;
  carts: number;
  purchases: number;
  conversion_rate: number;
}

interface CategoryRow {
  category_l1: string;
  category_l2: string;
  category_l3: string;
  total_events: number;
  views: number;
  carts: number;
  purchases: number;
  revenue: number;
  conversion_rate: number;
  cart_to_purchase_rate: number;
}

interface ProductRow {
  product_id: string | null;
  brand: string;
  category_l1: string;
  category_l2: string;
  category_l3: string;
  revenue: number;
  views: number;
  carts: number;
  purchases: number;
  conversion_rate: number;
}

type LeaderTab = "brands" | "categories" | "products";

// --- formatting -------------------------------------------------------------

function fmtMoney(v: number | null): string {
  if (v === null || v === undefined) return "-";
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function fmtInt(v: number | null): string {
  if (v === null || v === undefined) return "-";
  return v.toLocaleString();
}

function fmtPct(rate: number | null): string {
  if (rate === null || rate === undefined) return "-";
  return `${(rate * 100).toFixed(2)}%`;
}

// Round numeric cells for the leaderboard tables so they read cleanly.
function roundRows<T extends Record<string, unknown>>(rows: T[]): Array<Record<string, unknown>> {
  return rows.map((row) => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      out[k] = typeof v === "number" && !Number.isInteger(v) ? Number(v.toFixed(4)) : v;
    }
    return out;
  });
}

const BRAND_COLS = ["brand", "revenue", "views", "carts", "purchases", "conversion_rate"];
const CATEGORY_COLS = [
  "category_l1", "category_l2", "category_l3",
  "total_events", "views", "carts", "purchases", "revenue",
  "conversion_rate", "cart_to_purchase_rate",
];
const PRODUCT_COLS = [
  "product_id", "brand", "category_l1", "category_l2", "category_l3",
  "revenue", "views", "carts", "purchases", "conversion_rate",
];

export default function DashboardPage() {
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [limit, setLimit] = useState(10);

  const [overview, setOverview] = useState<Overview | null>(null);
  const [revenue, setRevenue] = useState<RevenuePoint[]>([]);
  const [brands, setBrands] = useState<BrandRow[]>([]);
  const [categories, setCategories] = useState<CategoryRow[]>([]);
  const [products, setProducts] = useState<ProductRow[]>([]);

  const [tab, setTab] = useState<LeaderTab>("brands");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadAll = useCallback(async () => {
    setLoading(true);
    setError(null);
    const range = new URLSearchParams();
    if (startDate) range.set("start_date", startDate);
    if (endDate) range.set("end_date", endDate);
    const rankParams = new URLSearchParams(range);
    rankParams.set("limit", String(limit));
    const overviewQs = endDate ? `?as_of_date=${endDate}` : "";

    try {
      const [ov, rev, br, cat, prod] = await Promise.all([
        api.get<Overview>(`/metrics/overview${overviewQs}`),
        api.get<RevenuePoint[]>(`/metrics/revenue?${range.toString()}`),
        api.get<BrandRow[]>(`/metrics/brands?${rankParams.toString()}`),
        api.get<CategoryRow[]>(`/metrics/categories?${rankParams.toString()}`),
        api.get<ProductRow[]>(`/metrics/products?${rankParams.toString()}`),
      ]);
      setOverview(ov);
      setRevenue(rev);
      setBrands(br);
      setCategories(cat);
      setProducts(prod);
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.message
          : "Could not load dashboard metrics. Trino or the Gold tables may be unavailable.",
      );
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate, limit]);

  useEffect(() => {
    void loadAll();
    // Initial load only; subsequent loads are triggered by "Apply".
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const revenueColumns = useMemo(
    () => ["event_date", "total_revenue", "total_events", "total_purchases", "conversion_rate", "cart_to_purchase_rate"],
    [],
  );

  const leaderboard = useMemo(() => {
    if (tab === "brands") return { cols: BRAND_COLS, rows: roundRows(brands as unknown as Record<string, unknown>[]) };
    if (tab === "categories") return { cols: CATEGORY_COLS, rows: roundRows(categories as unknown as Record<string, unknown>[]) };
    return { cols: PRODUCT_COLS, rows: roundRows(products as unknown as Record<string, unknown>[]) };
  }, [tab, brands, categories, products]);

  return (
    <div className="mx-auto flex max-w-7xl flex-col gap-4">
      <header className="flex flex-col justify-between gap-3 sm:flex-row sm:items-end">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">Dashboard</h1>
          <p className="text-xs text-text-secondary">
            KPIs and rankings from the Gold daily summaries
            {overview?.event_date ? ` · as of ${overview.event_date}` : ""}.
          </p>
        </div>
        <div className="flex flex-wrap items-end gap-2">
          <label className="flex flex-col text-[11px] text-text-secondary">
            From
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="rounded-md border border-border bg-surface px-2 py-1 text-sm text-text-primary focus:border-accent focus:outline-none"
            />
          </label>
          <label className="flex flex-col text-[11px] text-text-secondary">
            To
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="rounded-md border border-border bg-surface px-2 py-1 text-sm text-text-primary focus:border-accent focus:outline-none"
            />
          </label>
          <label className="flex flex-col text-[11px] text-text-secondary">
            Top N
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="rounded-md border border-border bg-surface px-2 py-1 text-sm text-text-primary focus:border-accent focus:outline-none"
            >
              {[5, 10, 20, 50, 100].map((n) => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </label>
          <button
            type="button"
            onClick={() => void loadAll()}
            disabled={loading}
            className="inline-flex items-center gap-1 rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-white hover:opacity-90 disabled:opacity-60"
          >
            <RotateCcw className={cn("h-3.5 w-3.5", loading && "animate-spin")} aria-hidden="true" />
            Apply
          </button>
        </div>
      </header>

      {error && (
        <div role="alert" className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error">
          <div className="flex items-center justify-between gap-2">
            <span className="inline-flex items-center gap-2 font-medium">
              <AlertTriangle className="h-4 w-4" aria-hidden="true" />
              Dashboard unavailable
            </span>
            <button
              type="button"
              onClick={() => void loadAll()}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-3 xl:grid-cols-6">
        <Kpi icon={BadgeDollarSign} label="Revenue (day)" value={loading ? null : fmtMoney(overview?.today_revenue ?? null)} />
        <Kpi icon={Activity} label="Events (day)" value={loading ? null : fmtInt(overview?.today_events ?? null)} />
        <Kpi icon={ShoppingCart} label="Purchases (day)" value={loading ? null : fmtInt(overview?.today_purchases ?? null)} />
        <Kpi icon={Percent} label="Conversion (day)" value={loading ? null : fmtPct(overview?.today_conversion_rate ?? null)} />
        <Kpi icon={TrendingUp} label="Revenue (MTD)" value={loading ? null : fmtMoney(overview?.mtd_revenue ?? null)} />
        <Kpi
          icon={Package}
          label="Top brand (MTD)"
          value={loading ? null : overview?.top_brand_mtd || "-"}
          sub={overview?.top_brand_mtd_revenue != null ? fmtMoney(overview.top_brand_mtd_revenue) : undefined}
        />
      </div>

      {/* Revenue time series */}
      <section className="rounded-lg border border-border bg-surface p-4 shadow-card">
        <div className="mb-3 flex items-center gap-1.5 text-sm font-medium text-text-primary">
          <BarChart3 className="h-4 w-4 text-accent" aria-hidden="true" />
          Revenue over time
        </div>
        {revenue.length > 0 ? (
          <ChartPanel
            columns={revenueColumns}
            rows={revenue as unknown as Array<Record<string, unknown>>}
            suggestion={{ chart_type: "line", x: "event_date", y: "total_revenue" }}
          />
        ) : (
          <p className="text-sm text-text-secondary">{loading ? "Loading..." : "No revenue data for this range."}</p>
        )}
      </section>

      {/* Leaderboards */}
      <section className="rounded-lg border border-border bg-surface shadow-card">
        <div role="tablist" className="flex items-center gap-1 border-b border-border px-2 pt-2">
          {([
            { key: "brands", label: "Top brands", icon: TrendingUp },
            { key: "categories", label: "Categories", icon: Layers },
            { key: "products", label: "Products", icon: Package },
          ] as Array<{ key: LeaderTab; label: string; icon: typeof TrendingUp }>).map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={tab === key}
              onClick={() => setTab(key)}
              className={cn(
                "inline-flex items-center gap-1 rounded-t-md px-3 py-1.5 text-xs",
                tab === key
                  ? "border border-b-0 border-border bg-background text-text-primary"
                  : "text-text-secondary hover:text-text-primary",
              )}
            >
              <Icon className="h-3.5 w-3.5" aria-hidden="true" />
              {label}
            </button>
          ))}
        </div>
        <div className="p-4">
          {leaderboard.rows.length > 0 ? (
            <TablePanel columns={leaderboard.cols} rows={leaderboard.rows} />
          ) : (
            <p className="text-sm text-text-secondary">{loading ? "Loading..." : "No rows for this range."}</p>
          )}
        </div>
      </section>
    </div>
  );
}

function Kpi({
  icon: Icon,
  label,
  value,
  sub,
}: {
  icon: typeof Activity;
  label: string;
  value: string | null;
  sub?: string;
}) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-border bg-surface p-3 shadow-card">
      <span className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-accent/10 text-accent">
        <Icon className="h-4 w-4" aria-hidden="true" />
      </span>
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-wide text-text-secondary">{label}</p>
        {value === null ? (
          <span className="mt-1 block h-4 w-16 animate-pulse rounded bg-border" aria-hidden="true" />
        ) : (
          <p className="truncate text-base font-semibold text-text-primary" title={value}>{value}</p>
        )}
        {sub && <p className="truncate text-[11px] text-text-secondary">{sub}</p>}
      </div>
    </div>
  );
}
