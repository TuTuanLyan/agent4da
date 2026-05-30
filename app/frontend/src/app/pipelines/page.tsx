"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { RotateCcw } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { PipelineCard, type PipelineRollupRow } from "@/components/pipelines/PipelineCard";

export default function PipelinesPage() {
  const [rows, setRows] = useState<PipelineRollupRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async (showSpinner = false) => {
    if (showSpinner) setLoading(true);
    setRefreshing(true);
    setError(null);
    try {
      const res = await api.get<PipelineRollupRow[]>("/pipelines");
      setRows(res);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to load pipelines.");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void load(true);
    pollRef.current = setInterval(() => void load(false), 15_000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [load]);

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <header className="flex items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">Pipelines</h1>
          <p className="text-xs text-text-secondary">
            Bronze / Silver / Gold / Metadata health. Auto-refresh every 15s.
          </p>
        </div>
        <button
          type="button"
          onClick={() => void load(false)}
          disabled={refreshing}
          className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-text-secondary hover:text-text-primary disabled:opacity-50"
        >
          <RotateCcw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin" : ""}`} aria-hidden="true" />
          Refresh
        </button>
      </header>

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Could not load pipelines</span>
            <button
              type="button"
              onClick={() => void load(true)}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {loading && !rows ? (
        <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
          Loading...
        </div>
      ) : (
        rows && (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {rows.map((row) => (
              <PipelineCard
                key={row.dag_id}
                rollup={row}
                onTriggered={() => void load(false)}
              />
            ))}
          </div>
        )
      )}
    </div>
  );
}
