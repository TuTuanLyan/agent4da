"use client";

import { CopyButton } from "./CopyButton";

export interface CatalogColumnRow {
  column_name: string;
  data_type: string | null;
  meaning: string | null;
  business_terms: string | null;
  example_usage: string | null;
}

interface Props {
  rows: CatalogColumnRow[];
}

export function ColumnList({ rows }: Props) {
  if (!rows.length) {
    return (
      <p className="rounded-md border border-border bg-surface p-4 text-center text-xs text-text-secondary">
        No columns recorded for this table.
      </p>
    );
  }
  return (
    <div className="overflow-hidden rounded-lg border border-border bg-surface shadow-card">
      <table className="zebra-table min-w-full text-sm">
        <thead className="bg-surface">
          <tr>
            <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Column
            </th>
            <th className="w-32 px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Type
            </th>
            <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wide text-text-secondary">
              Meaning &amp; terms
            </th>
            <th className="w-16 px-3 py-2"></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((c) => (
            <tr key={c.column_name} className="border-b border-border/50 last:border-0 align-top">
              <td className="px-3 py-2 font-mono text-xs text-text-primary">
                {c.column_name}
              </td>
              <td className="px-3 py-2">
                <span className="inline-flex rounded border border-border bg-background px-1.5 py-0.5 font-mono text-[11px] text-text-secondary">
                  {c.data_type ?? "-"}
                </span>
              </td>
              <td className="px-3 py-2 text-xs text-text-secondary">
                {c.meaning && <p className="text-text-primary">{c.meaning}</p>}
                {c.business_terms && (
                  <p className="mt-1">
                    <span className="text-[10px] uppercase text-text-secondary">Terms:</span>{" "}
                    {c.business_terms}
                  </p>
                )}
                {c.example_usage && (
                  <pre className="mt-2 overflow-x-auto rounded border border-border bg-background p-2 font-mono text-[11px] text-text-primary">
                    {c.example_usage}
                  </pre>
                )}
              </td>
              <td className="px-2 py-2 text-right">
                <CopyButton value={c.column_name} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
