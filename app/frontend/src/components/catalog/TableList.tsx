"use client";

import Link from "next/link";
import { CopyButton } from "./CopyButton";
import { KindBadge, type TableKind } from "./KindBadge";

export interface CatalogTableRow {
  table_name: string;
  display_name: string | null;
  purpose: string | null;
  grain: string | null;
  use_for: string | null;
  query_notes: string | null;
  kind: TableKind;
  column_count: number;
}

interface Props {
  rows: CatalogTableRow[];
}

export function TableList({ rows }: Props) {
  if (!rows.length) {
    return (
      <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
        No tables match.
      </div>
    );
  }
  return (
    <div className="overflow-hidden rounded-lg border border-border bg-surface shadow-card">
      <table className="zebra-table min-w-full text-sm">
        <thead className="bg-surface">
          <tr>
            <th className="w-20 px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Kind
            </th>
            <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Table
            </th>
            <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Purpose
            </th>
            <th className="w-20 px-3 py-2 text-right text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Cols
            </th>
            <th className="w-20 px-3 py-2"></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.table_name} className="border-b border-border/50 last:border-0">
              <td className="px-3 py-1.5">
                <KindBadge kind={r.kind} />
              </td>
              <td className="px-3 py-1.5">
                <Link
                  href={`/catalog/${encodeURIComponent(r.table_name)}`}
                  className="font-mono text-xs text-text-primary hover:text-accent"
                  title={r.display_name ?? undefined}
                >
                  {r.table_name}
                </Link>
                {r.display_name && (
                  <p className="text-[11px] text-text-secondary">{r.display_name}</p>
                )}
              </td>
              <td
                className="max-w-[40ch] px-3 py-1.5 text-xs text-text-secondary"
                title={r.purpose ?? undefined}
              >
                <span className="block truncate">{r.purpose ?? "-"}</span>
              </td>
              <td className="px-3 py-1.5 text-right tabular-nums text-text-secondary">
                {r.column_count}
              </td>
              <td className="px-2 py-1.5 text-right">
                <CopyButton value={r.table_name} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
