/**
 * Mirror of app/backend/chart/heuristics.py. The UI uses this when the user
 * toggles Auto back on without a network round-trip.
 *
 * Rules (first match wins):
 *  1. 1 date col + 1 numeric col   -> line
 *  2. 1 date col + N numeric       -> stacked line
 *  3. categorical (<=20) + numeric -> bar
 *  4. small categorical (<=8) + share/pct/rate numeric -> pie
 *  5. 2 numeric                    -> scatter
 *  6. otherwise                    -> table only
 */

import type { ChartSuggestion } from "./types";

const DATE_HINT = /(_date$|^date|_at$|^ts$|_ts$|_time$|^time$)/i;
const PCT_HINT = /(share|ratio|rate|pct|percent)/i;

type ColKind = "date" | "numeric" | "categorical" | "unknown";

function looksDate(value: unknown): boolean {
  if (value instanceof Date) return true;
  if (typeof value === "string") {
    // ISO-ish date / datetime quick check.
    return /^\d{4}-\d{2}-\d{2}/.test(value);
  }
  return false;
}

function classifyColumn(name: string, samples: unknown[]): ColKind {
  const nonNull = samples.filter((v) => v !== null && v !== undefined);
  if (nonNull.length === 0) {
    return DATE_HINT.test(name) ? "date" : "unknown";
  }
  if (nonNull.every(looksDate)) return "date";
  if (nonNull.every((v) => typeof v === "number" && Number.isFinite(v))) return "numeric";
  return "categorical";
}

function distinctCount(samples: unknown[]): number {
  return new Set(samples.filter((v) => v !== null && v !== undefined)).size;
}

export function suggestChart(
  columns: string[],
  rows: Array<Record<string, unknown>>,
): ChartSuggestion | null {
  if (!columns.length || !rows.length) return null;
  const sample = rows.slice(0, 200);

  const samples: Record<string, unknown[]> = {};
  for (const c of columns) samples[c] = [];
  for (const r of sample) {
    for (const c of columns) samples[c].push(r[c]);
  }

  const kinds: Record<string, ColKind> = {};
  for (const c of columns) kinds[c] = classifyColumn(c, samples[c]);

  const dateCols = columns.filter((c) => kinds[c] === "date");
  const numericCols = columns.filter((c) => kinds[c] === "numeric");
  const categoricalCols = columns.filter((c) => kinds[c] === "categorical");

  if (dateCols.length === 1 && numericCols.length >= 1) {
    const x = dateCols[0];
    if (numericCols.length === 1) return { chart_type: "line", x, y: numericCols[0] };
    return {
      chart_type: "line",
      x,
      y: numericCols[0],
      series: numericCols.slice(1),
    };
  }

  if (categoricalCols.length === 1 && numericCols.length >= 1) {
    const cat = categoricalCols[0];
    const catDistinct = distinctCount(samples[cat]);
    const partOf = numericCols.find((c) => PCT_HINT.test(c));
    if (catDistinct <= 8 && partOf) return { chart_type: "pie", x: cat, y: partOf };
    if (catDistinct <= 20) return { chart_type: "bar", x: cat, y: numericCols[0], sort: "desc" };
  }

  if (!dateCols.length && numericCols.length >= 2 && !categoricalCols.length) {
    return { chart_type: "scatter", x: numericCols[0], y: numericCols[1] };
  }

  return null;
}
