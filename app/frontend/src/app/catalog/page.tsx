"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Search } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { TableList, type CatalogTableRow } from "@/components/catalog/TableList";
import type { TableKind } from "@/components/catalog/KindBadge";
import { cn } from "@/lib/utils";

interface SearchHit {
  kind: "table" | "column";
  table_name: string;
  column_name: string | null;
  display_name: string | null;
  snippet: string;
}

interface SearchResponse {
  query: string;
  hits: SearchHit[];
}

const KIND_OPTIONS: ("all" | TableKind)[] = [
  "all",
  "fact",
  "dimension",
  "summary",
  "semantic",
];

function useDebouncedValue<T>(value: T, ms: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return debounced;
}

export default function CatalogListPage() {
  const [tables, setTables] = useState<CatalogTableRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [q, setQ] = useState("");
  const [kindFilter, setKindFilter] = useState<"all" | TableKind>("all");
  const debouncedQ = useDebouncedValue(q.trim(), 200);
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [searching, setSearching] = useState(false);

  const fetchTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.get<CatalogTableRow[]>("/catalog/tables");
      setTables(res);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to load catalog.");
      setTables(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void fetchTables();
  }, [fetchTables]);

  useEffect(() => {
    if (!debouncedQ) {
      setHits([]);
      return;
    }
    let cancelled = false;
    setSearching(true);
    api
      .get<SearchResponse>(`/catalog/search?q=${encodeURIComponent(debouncedQ)}&limit=20`)
      .then((res) => {
        if (!cancelled) setHits(res.hits);
      })
      .catch(() => {
        if (!cancelled) setHits([]);
      })
      .finally(() => {
        if (!cancelled) setSearching(false);
      });
    return () => {
      cancelled = true;
    };
  }, [debouncedQ]);

  const filtered = useMemo(() => {
    if (!tables) return [];
    if (kindFilter === "all") return tables;
    return tables.filter((t) => t.kind === kindFilter);
  }, [tables, kindFilter]);

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <header>
        <h1 className="text-lg font-semibold text-text-primary">Catalog</h1>
        <p className="text-xs text-text-secondary">
          Browse semantic Gold tables and columns. Read-only.
        </p>
      </header>

      <div className="grid grid-cols-1 gap-3 rounded-lg border border-border bg-surface p-3 shadow-card md:grid-cols-[1fr_auto]">
        <div>
          <label htmlFor="catalog-q" className="block text-[11px] text-text-secondary">
            Search tables and columns
          </label>
          <div className="relative">
            <Search
              className="pointer-events-none absolute left-2 top-1.5 h-3.5 w-3.5 text-text-secondary"
              aria-hidden="true"
            />
            <input
              id="catalog-q"
              type="search"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="brand, doanh thu, daily_event_summary, ..."
              className="block w-full rounded-md border border-border bg-background pl-7 pr-2 py-1.5 text-xs text-text-primary"
            />
          </div>
        </div>
        <div className="flex items-end gap-1">
          {KIND_OPTIONS.map((k) => (
            <button
              key={k}
              type="button"
              onClick={() => setKindFilter(k)}
              aria-pressed={kindFilter === k}
              className={cn(
                "rounded-full border px-2.5 py-1 text-[11px] capitalize",
                kindFilter === k
                  ? "border-accent/40 bg-accent/10 text-accent"
                  : "border-border text-text-secondary hover:text-text-primary",
              )}
            >
              {k}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Catalog source unavailable</span>
            <button
              type="button"
              onClick={fetchTables}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {debouncedQ && (
        <section className="rounded-lg border border-border bg-surface p-3 shadow-card">
          <p className="text-[11px] uppercase tracking-wide text-text-secondary">
            {searching ? "Searching..." : `${hits.length} matches for "${debouncedQ}"`}
          </p>
          {hits.length > 0 && (
            <ul className="mt-2 divide-y divide-border/60">
              {hits.map((h, i) => (
                <li key={`${h.table_name}-${h.column_name ?? "table"}-${i}`} className="py-1.5">
                  <Link
                    href={`/catalog/${encodeURIComponent(h.table_name)}`}
                    className="block text-xs text-text-primary hover:text-accent"
                  >
                    <span className="font-mono">
                      {h.table_name}
                      {h.column_name ? `.${h.column_name}` : ""}
                    </span>
                    {h.snippet && (
                      <span className="ml-2 text-text-secondary">- {h.snippet}</span>
                    )}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}

      {loading && !tables ? (
        <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
          Loading catalog...
        </div>
      ) : (
        tables && <TableList rows={filtered} />
      )}
    </div>
  );
}
