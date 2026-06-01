"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { suggestChart } from "@/lib/chart-pick";
import type { ChartSuggestion, ChartType } from "@/lib/types";
import { cn } from "@/lib/utils";

interface Props {
  columns: string[];
  rows: Array<Record<string, unknown>>;
  suggestion: ChartSuggestion | null;
  initialType?: ChartType;
}

const TYPES: ChartType[] = ["auto", "bar", "line", "pie", "table"];

const PALETTE = [
  "var(--accent)",
  "var(--accent-2)",
  "var(--warning)",
  "var(--success)",
  "var(--error)",
  "#8B5CF6",
];

function resolveChart(
  type: ChartType,
  columns: string[],
  rows: Array<Record<string, unknown>>,
  suggestion: ChartSuggestion | null,
): ChartSuggestion | null {
  if (type === "table") return null;
  if (type === "auto") return suggestion ?? suggestChart(columns, rows);
  // Forced override: try to keep the same x/y from the suggestion if shape
  // permits; else fall back to first two columns.
  const base = suggestion ?? suggestChart(columns, rows);
  if (base) {
    if (type === "scatter") {
      return { ...base, chart_type: "scatter" };
    }
    if (type === "bar" || type === "line" || type === "pie") {
      return { ...base, chart_type: type };
    }
  }
  if (columns.length >= 2) {
    return {
      chart_type: type === "scatter" ? "scatter" : (type as "bar" | "line" | "pie"),
      x: columns[0],
      y: columns[1],
    };
  }
  return null;
}

export function ChartPanel({ columns, rows, suggestion, initialType = "auto" }: Props) {
  const [type, setType] = useState<ChartType>(initialType);
  const chart = useMemo(
    () => resolveChart(type, columns, rows, suggestion),
    [type, columns, rows, suggestion],
  );

  if (!rows.length) {
    return <p className="text-sm text-text-secondary">No rows to plot.</p>;
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-1 rounded-md border border-border bg-surface p-1">
        {TYPES.map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setType(t)}
            aria-pressed={type === t}
            className={cn(
              "rounded px-2.5 py-1 text-xs capitalize",
              type === t
                ? "bg-accent/10 text-accent"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            {t}
          </button>
        ))}
        {chart && (
          <span className="ml-auto pr-2 text-[11px] text-text-secondary">
            x: {chart.x} - y: {chart.y}
          </span>
        )}
      </div>

      <div className="h-72 w-full">
        <ResponsiveContainer width="100%" height="100%">
          {renderChart(chart, rows)}
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function renderChart(
  chart: ChartSuggestion | null,
  rows: Array<Record<string, unknown>>,
): React.ReactElement {
  if (!chart) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-text-secondary">
        No chart suggested for this result shape. Try the Table tab.
      </div>
    );
  }

  const axisStyle = { fontSize: 11, fill: "var(--text-secondary)" };
  const tooltipStyle = {
    backgroundColor: "var(--elevated)",
    border: "1px solid var(--border)",
    color: "var(--text-primary)",
    fontSize: 12,
  };

  if (chart.chart_type === "line") {
    return (
      <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
        <XAxis dataKey={chart.x} tick={axisStyle} />
        <YAxis tick={axisStyle} />
        <Tooltip contentStyle={tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Line
          type="monotone"
          dataKey={chart.y}
          stroke={PALETTE[0]}
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4 }}
        />
        {chart.series?.map((s, i) => (
          <Line
            key={s}
            type="monotone"
            dataKey={s}
            stroke={PALETTE[(i + 1) % PALETTE.length]}
            strokeWidth={2}
            dot={false}
          />
        ))}
      </LineChart>
    );
  }

  if (chart.chart_type === "bar") {
    const sorted = chart.sort === "desc"
      ? [...rows].sort((a, b) => Number(b[chart.y] ?? 0) - Number(a[chart.y] ?? 0))
      : rows;
    return (
      <BarChart data={sorted} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
        <XAxis dataKey={chart.x} tick={axisStyle} />
        <YAxis tick={axisStyle} />
        <Tooltip contentStyle={tooltipStyle} />
        <Bar dataKey={chart.y} fill={PALETTE[0]} radius={[4, 4, 0, 0]} />
      </BarChart>
    );
  }

  if (chart.chart_type === "pie") {
    const data = rows.map((r) => ({
      name: String(r[chart.x] ?? ""),
      value: Number(r[chart.y] ?? 0),
    }));
    return (
      <PieChart>
        <Tooltip contentStyle={tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Pie data={data} dataKey="value" nameKey="name" outerRadius={90}>
          {data.map((_, i) => (
            <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
          ))}
        </Pie>
      </PieChart>
    );
  }

  if (chart.chart_type === "scatter") {
    return (
      <ScatterChart margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
        <XAxis dataKey={chart.x} type="number" tick={axisStyle} />
        <YAxis dataKey={chart.y} type="number" tick={axisStyle} />
        <Tooltip contentStyle={tooltipStyle} />
        <Scatter data={rows} fill={PALETTE[0]} />
      </ScatterChart>
    );
  }

  return (
    <div className="flex h-full items-center justify-center text-sm text-text-secondary">
      Unsupported chart type.
    </div>
  );
}
