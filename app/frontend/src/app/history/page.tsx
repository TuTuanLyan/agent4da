"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { api, ApiError } from "@/lib/api";
import { HistoryFilters, type HistoryFilterState } from "@/components/history/HistoryFilters";
import { HistoryTable, type HistoryRow } from "@/components/history/HistoryTable";
import type { RunStatus } from "@/lib/types";

interface HistoryPage {
  items: HistoryRow[];
  total: number;
  page: number;
  limit: number;
  has_next: boolean;
}

const INITIAL: HistoryFilterState = {
  from: "",
  to: "",
  statuses: new Set<RunStatus>(),
  favoritesOnly: false,
  q: "",
};

export default function HistoryListPage() {
  const [filters, setFilters] = useState<HistoryFilterState>(INITIAL);
  const [page, setPage] = useState(1);
  const [data, setData] = useState<HistoryPage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const query = useMemo(() => {
    const params = new URLSearchParams();
    if (filters.from) params.set("from", filters.from);
    if (filters.to) params.set("to", filters.to);
    if (filters.statuses.size) params.set("status", [...filters.statuses].join(","));
    if (filters.favoritesOnly) params.set("favorite", "true");
    if (filters.q.trim()) params.set("q", filters.q.trim());
    params.set("page", String(page));
    params.set("limit", "25");
    return params.toString();
  }, [filters, page]);

  // Reset to page 1 when filters change.
  useEffect(() => {
    setPage(1);
  }, [filters]);

  const fetchPage = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.get<HistoryPage>(`/history?${query}`);
      setData(res);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to load history.");
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [query]);

  useEffect(() => {
    void fetchPage();
  }, [fetchPage]);

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <header className="flex items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">History</h1>
          <p className="text-xs text-text-secondary">
            Every question you ask is saved here. Star runs to pin them in the sidebar.
          </p>
        </div>
        {data && (
          <span className="text-[11px] text-text-secondary">
            {data.total.toLocaleString()} runs
          </span>
        )}
      </header>

      <HistoryFilters value={filters} onChange={setFilters} />

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Couldn&apos;t load history</span>
            <button
              type="button"
              onClick={fetchPage}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {loading && !data ? (
        <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
          Loading...
        </div>
      ) : (
        data && (
          <>
            <HistoryTable
              rows={data.items}
              onFavoriteChange={() => {
                /* StatusBadge is in-place updated by FavoriteToggle, no refetch needed. */
              }}
            />

            <div className="flex items-center justify-between text-[11px] text-text-secondary">
              <span>
                Page {data.page} - {data.items.length} of {data.total.toLocaleString()}
              </span>
              <div className="flex items-center gap-1">
                <button
                  type="button"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={data.page <= 1}
                  className="rounded border border-border bg-surface px-2 py-1 hover:text-text-primary disabled:opacity-50"
                >
                  Prev
                </button>
                <button
                  type="button"
                  onClick={() => setPage((p) => p + 1)}
                  disabled={!data.has_next}
                  className="rounded border border-border bg-surface px-2 py-1 hover:text-text-primary disabled:opacity-50"
                >
                  Next
                </button>
              </div>
            </div>
          </>
        )
      )}
    </div>
  );
}
