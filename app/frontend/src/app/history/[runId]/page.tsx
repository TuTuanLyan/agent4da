"use client";

import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { ChevronLeft, Play } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { ResultTabs } from "@/components/ask/ResultTabs";
import { FavoriteToggle } from "@/components/history/FavoriteToggle";
import { StatusBadge } from "@/components/history/StatusBadge";
import { formatMs, formatNumber, formatRelative } from "@/lib/format";
import type { AskResult } from "@/lib/types";

export default function HistoryDetailPage() {
  const params = useParams<{ runId: string }>();
  const router = useRouter();
  const runId = params.runId;
  const [run, setRun] = useState<AskResult | null>(null);
  const [favorite, setFavorite] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchRun = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.get<AskResult & { is_favorite?: boolean }>(
        `/history/${runId}`,
      );
      setRun(res);
      // The AskResponse schema does not include is_favorite directly today,
      // so default to false; the FavoriteToggle keeps its own optimistic state.
      setFavorite(Boolean((res as { is_favorite?: boolean }).is_favorite));
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.status === 404
            ? "Run not found or no longer accessible."
            : err.message
          : "Failed to load run.",
      );
      setRun(null);
    } finally {
      setLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    if (!runId) return;
    void fetchRun();
  }, [runId, fetchRun]);

  function onRerun() {
    if (!run) return;
    const q = encodeURIComponent(run.question);
    router.push(`/ask?question=${q}`);
  }

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <header className="flex flex-wrap items-center gap-2">
        <Link
          href="/history"
          className="inline-flex items-center gap-1 text-xs text-text-secondary hover:text-text-primary"
        >
          <ChevronLeft className="h-3.5 w-3.5" aria-hidden="true" />
          Back to history
        </Link>
        {run && (
          <>
            <span className="ml-2">
              <StatusBadge status={run.status} />
            </span>
            <span className="text-[11px] text-text-secondary">
              {formatNumber(run.row_count)} rows - {formatMs(run.latency_ms)} -{" "}
              {formatRelative(run.created_at)}
            </span>
            <div className="ml-auto flex items-center gap-2">
              <FavoriteToggle
                runId={run.run_id}
                initial={favorite}
                onChange={setFavorite}
              />
              <button
                type="button"
                onClick={onRerun}
                className="inline-flex items-center gap-1 rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-white hover:opacity-90"
              >
                <Play className="h-3.5 w-3.5" aria-hidden="true" />
                Re-run this question
              </button>
            </div>
          </>
        )}
      </header>

      {run && (
        <section className="rounded-lg border border-border bg-surface p-4 shadow-card">
          <p className="text-[11px] uppercase tracking-wide text-text-secondary">
            Question
          </p>
          <p className="mt-1 text-sm text-text-primary">{run.question}</p>
        </section>
      )}

      {loading && !run && (
        <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
          Loading run...
        </div>
      )}

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Could not open run</span>
            <button
              type="button"
              onClick={fetchRun}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {run && <ResultTabs result={run} />}
    </div>
  );
}
