"use client";

import { useState } from "react";
import {
  Copy,
  Download,
  Play,
  RotateCcw,
  Star,
  StopCircle,
} from "lucide-react";
import { api, API_BASE_URL, ApiError } from "@/lib/api";
import { cn } from "@/lib/utils";
import type { AskResult } from "@/lib/types";

interface Props {
  result: AskResult | null;
  streaming: boolean;
  canRun: boolean;
  onRun: () => void;
  onStop: () => void;
}

export function ActionBar({ result, streaming, canRun, onRun, onStop }: Props) {
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [downloading, setDownloading] = useState(false);

  async function onCopySql() {
    if (!result?.generated_sql) return;
    try {
      await navigator.clipboard.writeText(result.generated_sql);
    } catch {
      /* ignore */
    }
  }

  async function onExportCsv() {
    if (!result) return;
    setDownloading(true);
    try {
      const res = await api.get<{ token: string }>(
        `/agent/runs/${result.run_id}/export-token`,
      );
      const url = `${API_BASE_URL}/agent/runs/${result.run_id}/export.csv?token=${encodeURIComponent(res.token)}`;
      window.location.href = url;
    } catch (err) {
      if (err instanceof ApiError) {
        // eslint-disable-next-line no-alert
        alert(err.message);
      }
    } finally {
      setDownloading(false);
    }
  }

  async function onSave() {
    if (!result) return;
    setSaving(true);
    try {
      // Phase 4 wires this endpoint; for Phase 3 we just attempt and ignore 404.
      await api.post(`/history/${result.run_id}/favorite`);
      setSaved(true);
      setTimeout(() => setSaved(false), 1500);
    } catch {
      /* History endpoints land in Phase 4 */
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-surface p-2 shadow-card">
      {streaming ? (
        <button
          type="button"
          onClick={onStop}
          className="inline-flex items-center gap-1 rounded-md border border-warning/40 bg-warning/10 px-3 py-1.5 text-xs font-medium text-warning hover:bg-warning/20"
        >
          <StopCircle className="h-3.5 w-3.5" aria-hidden="true" />
          Stop
        </button>
      ) : (
        <button
          type="button"
          onClick={onRun}
          disabled={!canRun}
          className="inline-flex items-center gap-1 rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-white hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
        >
          <Play className="h-3.5 w-3.5" aria-hidden="true" />
          Run
        </button>
      )}

      <button
        type="button"
        onClick={onRun}
        disabled={!result || streaming}
        className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-3 py-1.5 text-xs text-text-secondary hover:text-text-primary disabled:opacity-50"
      >
        <RotateCcw className="h-3.5 w-3.5" aria-hidden="true" />
        Retry
      </button>

      <button
        type="button"
        onClick={onCopySql}
        disabled={!result?.generated_sql}
        className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-3 py-1.5 text-xs text-text-secondary hover:text-text-primary disabled:opacity-50"
      >
        <Copy className="h-3.5 w-3.5" aria-hidden="true" />
        Copy SQL
      </button>

      <button
        type="button"
        onClick={onExportCsv}
        disabled={!result || result.row_count === 0 || downloading}
        className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-3 py-1.5 text-xs text-text-secondary hover:text-text-primary disabled:opacity-50"
      >
        <Download className="h-3.5 w-3.5" aria-hidden="true" />
        Export CSV
      </button>

      <button
        type="button"
        onClick={onSave}
        disabled={!result || saving}
        className={cn(
          "inline-flex items-center gap-1 rounded-md border border-border bg-surface px-3 py-1.5 text-xs",
          saved ? "text-warning" : "text-text-secondary hover:text-text-primary",
          "disabled:opacity-50",
        )}
      >
        <Star className={cn("h-3.5 w-3.5", saved && "fill-current")} aria-hidden="true" />
        {saved ? "Saved" : "Save"}
      </button>
    </div>
  );
}
