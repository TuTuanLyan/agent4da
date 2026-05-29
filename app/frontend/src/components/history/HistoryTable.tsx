"use client";

import Link from "next/link";
import { FavoriteToggle } from "./FavoriteToggle";
import { StatusBadge } from "./StatusBadge";
import { formatMs, formatNumber, formatRelative } from "@/lib/format";
import type { RunStatus } from "@/lib/types";

export interface HistoryRow {
  run_id: string;
  question: string;
  status: RunStatus;
  guard_status: string | null;
  row_count: number;
  latency_ms: number | null;
  is_favorite: boolean;
  has_summary: boolean;
  created_at: string;
}

interface Props {
  rows: HistoryRow[];
  onFavoriteChange?: (runId: string, next: boolean) => void;
}

export function HistoryTable({ rows, onFavoriteChange }: Props) {
  if (!rows.length) {
    return (
      <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
        No runs match these filters.
      </div>
    );
  }
  return (
    <div className="overflow-hidden rounded-lg border border-border bg-surface shadow-card">
      <table className="zebra-table min-w-full text-sm">
        <thead className="bg-surface">
          <tr>
            <th className="w-8 px-3 py-2"></th>
            <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Status
            </th>
            <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Question
            </th>
            <th className="px-3 py-2 text-right text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Rows
            </th>
            <th className="px-3 py-2 text-right text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Latency
            </th>
            <th className="px-3 py-2 text-right text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              When
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr
              key={r.run_id}
              className="border-b border-border/50 last:border-0 hover:bg-background/60"
            >
              <td className="px-2 py-1.5">
                <FavoriteToggle
                  runId={r.run_id}
                  initial={r.is_favorite}
                  onChange={(next) => onFavoriteChange?.(r.run_id, next)}
                />
              </td>
              <td className="px-3 py-1.5">
                <StatusBadge status={r.status} />
              </td>
              <td className="px-3 py-1.5">
                <Link
                  href={`/history/${r.run_id}`}
                  className="block max-w-[60ch] truncate text-text-primary hover:text-accent"
                  title={r.question}
                >
                  {r.question || "(empty)"}
                </Link>
              </td>
              <td className="px-3 py-1.5 text-right tabular-nums text-text-secondary">
                {formatNumber(r.row_count)}
              </td>
              <td className="px-3 py-1.5 text-right tabular-nums text-text-secondary">
                {formatMs(r.latency_ms)}
              </td>
              <td
                className="px-3 py-1.5 text-right text-text-secondary"
                title={new Date(r.created_at).toLocaleString()}
              >
                {formatRelative(r.created_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
