"use client";

import Link from "next/link";
import {
  AlertCircle,
  Clock,
  Copy,
  FileBarChart,
  Play,
  Terminal,
} from "lucide-react";
import { useState } from "react";
import { api, ApiError } from "@/lib/api";
import { useAuth } from "@/components/auth/AuthProvider";
import { formatMs, formatNumber, formatRelative } from "@/lib/format";
import { cn } from "@/lib/utils";

export type Layer = "bronze" | "silver" | "gold" | "metadata";
type ScheduleValue = string | { __type?: string; value?: string | number | null } | null;

export interface PipelineRollupRow {
  dag_id: string;
  label: string;
  layer: Layer;
  schedule: ScheduleValue;
  is_paused: boolean;
  last_run_id: string | null;
  last_run_at: string | null;
  last_run_state: string | null;
  last_duration_sec: number | null;
  next_run_at: string | null;
  row_count_after_last_run: number | null;
  error: string | null;
}

interface Props {
  rollup: PipelineRollupRow;
  onTriggered?: () => void;
}

function dotColor(state: string | null, paused: boolean, error: string | null) {
  if (error) return "bg-error";
  if (paused) return "bg-text-secondary/50";
  if (state === "success") return "bg-success";
  if (state === "failed" || state === "upstream_failed") return "bg-error";
  if (state === "running" || state === "queued") return "bg-accent animate-pulse";
  return "bg-text-secondary/50";
}

const LAYER_TONE: Record<Layer, string> = {
  bronze: "border-warning/40 bg-warning/10 text-warning",
  silver: "border-border bg-surface text-text-secondary",
  gold: "border-accent-2/40 bg-accent-2/10 text-accent-2",
  metadata: "border-accent/40 bg-accent/10 text-accent",
};

function scheduleText(schedule: ScheduleValue): string | null {
  if (schedule == null) return null;
  if (typeof schedule === "string") return schedule;
  if (schedule.value != null) return String(schedule.value);
  if (schedule.__type) return schedule.__type;
  return null;
}

export function PipelineCard({ rollup, onTriggered }: Props) {
  const { user } = useAuth();
  const isAdmin = user?.role === "admin";
  const [triggering, setTriggering] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const schedule = scheduleText(rollup.schedule);

  async function copyDebug() {
    try {
      const res = await api.get<{ commands: string[] }>(
        `/pipelines/debug-command?dag=${encodeURIComponent(rollup.dag_id)}`,
      );
      const text = res.commands.join("\n");
      await navigator.clipboard.writeText(text);
    } catch {
      /* clipboard or auth failure - non-blocking */
    }
  }

  async function onTrigger() {
    if (!isAdmin || triggering) return;
    setTriggering(true);
    setTriggerError(null);
    try {
      await api.post(`/pipelines/${rollup.dag_id}/trigger`, { json: {} });
      onTriggered?.();
    } catch (err) {
      setTriggerError(
        err instanceof ApiError ? err.message : "Trigger failed.",
      );
    } finally {
      setTriggering(false);
    }
  }

  return (
    <article className="flex flex-col gap-3 rounded-lg border border-border bg-surface p-4 shadow-card">
      <header className="flex flex-wrap items-center gap-2">
        <span
          aria-hidden="true"
          className={cn("h-2.5 w-2.5 rounded-full", dotColor(rollup.last_run_state, rollup.is_paused, rollup.error))}
        />
        <h2 className="text-sm font-semibold text-text-primary">{rollup.label}</h2>
        <span
          className={cn(
            "inline-flex items-center rounded-md border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide",
            LAYER_TONE[rollup.layer],
          )}
        >
          {rollup.layer}
        </span>
        {rollup.is_paused && (
          <span className="rounded border border-border bg-background px-1.5 py-0.5 text-[10px] uppercase text-text-secondary">
            paused
          </span>
        )}
        <span className="ml-auto font-mono text-[10px] text-text-secondary">
          {rollup.dag_id}
        </span>
      </header>

      {rollup.error ? (
        <div className="flex items-start gap-2 rounded-md border border-error/40 bg-error/10 p-2 text-xs text-error">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5" aria-hidden="true" />
          <span>{rollup.error}</span>
        </div>
      ) : (
        <dl className="grid grid-cols-2 gap-2 text-xs">
          <Stat label="Last run">
            {rollup.last_run_at ? (
              <span title={new Date(rollup.last_run_at).toLocaleString()}>
                {formatRelative(rollup.last_run_at)}
              </span>
            ) : (
              <span className="text-text-secondary">never</span>
            )}
          </Stat>
          <Stat label="State">
            <span className={cn("font-medium", stateColor(rollup.last_run_state))}>
              {rollup.last_run_state ?? "-"}
            </span>
          </Stat>
          <Stat label="Duration">
            {rollup.last_duration_sec != null
              ? formatMs(Math.round(rollup.last_duration_sec * 1000))
              : "-"}
          </Stat>
          <Stat label="Schedule">
            <span className="truncate font-mono text-text-primary" title={schedule ?? undefined}>
              {schedule ?? "-"}
            </span>
          </Stat>
          {rollup.next_run_at && (
            <Stat label="Next">
              <span className="inline-flex items-center gap-1 text-text-secondary">
                <Clock className="h-3 w-3" aria-hidden="true" />
                {formatRelative(rollup.next_run_at)}
              </span>
            </Stat>
          )}
          <Stat label="Layer rows">
            <span className="inline-flex items-center gap-1">
              <FileBarChart className="h-3 w-3 text-text-secondary" aria-hidden="true" />
              {rollup.row_count_after_last_run != null
                ? formatNumber(rollup.row_count_after_last_run)
                : <span className="text-text-secondary">snapshot pending</span>}
            </span>
          </Stat>
        </dl>
      )}

      {triggerError && (
        <p className="rounded-md border border-error/40 bg-error/10 px-2 py-1 text-[11px] text-error">
          {triggerError}
        </p>
      )}

      <footer className="flex flex-wrap items-center gap-2">
        {rollup.last_run_id && (
          <Link
            href={`/pipelines/${rollup.dag_id}/runs/${encodeURIComponent(rollup.last_run_id)}`}
            className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-text-secondary hover:text-text-primary"
          >
            Open latest run
          </Link>
        )}
        <button
          type="button"
          onClick={copyDebug}
          className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-text-secondary hover:text-text-primary"
        >
          <Terminal className="h-3.5 w-3.5" aria-hidden="true" />
          Copy debug
        </button>
        {isAdmin && (
          <button
            type="button"
            onClick={onTrigger}
            disabled={triggering || !!rollup.error}
            className={cn(
              "ml-auto inline-flex items-center gap-1 rounded-md bg-accent px-2.5 py-1 text-xs font-medium text-white",
              "hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60",
            )}
          >
            <Play className="h-3.5 w-3.5" aria-hidden="true" />
            {triggering ? "Triggering..." : "Trigger"}
          </button>
        )}
      </footer>
    </article>
  );
}

function Stat({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] uppercase tracking-wide text-text-secondary">{label}</dt>
      <dd className="mt-0.5 truncate text-text-primary">{children}</dd>
    </div>
  );
}

function stateColor(state: string | null): string {
  if (state === "success") return "text-success";
  if (state === "failed" || state === "upstream_failed") return "text-error";
  if (state === "running" || state === "queued") return "text-accent";
  return "text-text-secondary";
}
