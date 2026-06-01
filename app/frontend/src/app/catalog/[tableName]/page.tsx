"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { ChevronLeft } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { ColumnList, type CatalogColumnRow } from "@/components/catalog/ColumnList";
import { CopyButton } from "@/components/catalog/CopyButton";
import { KindBadge, type TableKind } from "@/components/catalog/KindBadge";

interface CatalogTableDetail {
  table_name: string;
  display_name: string | null;
  purpose: string | null;
  grain: string | null;
  use_for: string | null;
  query_notes: string | null;
  kind: TableKind;
  column_count: number;
  columns: CatalogColumnRow[];
}

export default function CatalogDetailPage() {
  const params = useParams<{ tableName: string }>();
  const tableName = decodeURIComponent(params.tableName ?? "");
  const [detail, setDetail] = useState<CatalogTableDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    if (!tableName) return;
    setLoading(true);
    setError(null);
    try {
      const res = await api.get<CatalogTableDetail>(
        `/catalog/tables/${encodeURIComponent(tableName)}`,
      );
      setDetail(res);
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.status === 404
            ? "Unknown table."
            : err.message
          : "Failed to load table.",
      );
      setDetail(null);
    } finally {
      setLoading(false);
    }
  }, [tableName]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <header className="flex flex-wrap items-center gap-2">
        <Link
          href="/catalog"
          className="inline-flex items-center gap-1 text-xs text-text-secondary hover:text-text-primary"
        >
          <ChevronLeft className="h-3.5 w-3.5" aria-hidden="true" />
          Back to catalog
        </Link>
      </header>

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Could not open table</span>
            <button
              type="button"
              onClick={load}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {loading && !detail && (
        <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
          Loading...
        </div>
      )}

      {detail && (
        <>
          <section className="rounded-lg border border-border bg-surface p-4 shadow-card">
            <div className="flex flex-wrap items-center gap-2">
              <KindBadge kind={detail.kind} />
              <span className="font-mono text-sm text-text-primary">{detail.table_name}</span>
              <CopyButton value={detail.table_name} label="Copy" />
              {detail.display_name && (
                <span className="text-xs text-text-secondary">- {detail.display_name}</span>
              )}
              <span className="ml-auto text-[11px] text-text-secondary">
                {detail.column_count} columns
              </span>
            </div>

            <dl className="mt-4 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
              {detail.purpose && (
                <div>
                  <dt className="text-[11px] uppercase tracking-wide text-text-secondary">
                    Purpose
                  </dt>
                  <dd className="mt-0.5 text-text-primary">{detail.purpose}</dd>
                </div>
              )}
              {detail.grain && (
                <div>
                  <dt className="text-[11px] uppercase tracking-wide text-text-secondary">
                    Grain
                  </dt>
                  <dd className="mt-0.5 text-text-primary">{detail.grain}</dd>
                </div>
              )}
              {detail.use_for && (
                <div className="sm:col-span-2">
                  <dt className="text-[11px] uppercase tracking-wide text-text-secondary">
                    Use for
                  </dt>
                  <dd className="mt-0.5 text-text-primary">{detail.use_for}</dd>
                </div>
              )}
              {detail.query_notes && (
                <div className="sm:col-span-2">
                  <dt className="text-[11px] uppercase tracking-wide text-text-secondary">
                    Query notes
                  </dt>
                  <dd className="mt-0.5 text-text-primary">{detail.query_notes}</dd>
                </div>
              )}
            </dl>
          </section>

          <ColumnList rows={detail.columns} />
        </>
      )}
    </div>
  );
}
