"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { ChevronLeft } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { LogViewer } from "@/components/pipelines/LogViewer";
import { RunStateBadge } from "@/components/pipelines/RunStateBadge";
import {
  TaskInstanceList,
  type TaskInstanceRow,
} from "@/components/pipelines/TaskInstanceList";
import { formatMs, formatRelative } from "@/lib/format";

interface PipelineRun {
  dag_id: string;
  run_id: string;
  logical_date: string | null;
  start_date: string | null;
  end_date: string | null;
  state: string | null;
  duration_sec: number | null;
  run_type: string | null;
  note: string | null;
}

interface TaskLogResponse {
  dag_id: string;
  run_id: string;
  task_id: string;
  try_number: number;
  content: string;
  truncated: boolean;
  size_bytes: number;
}

export default function PipelineRunDetailPage() {
  const params = useParams<{ dagId: string; runId: string }>();
  const dagId = decodeURIComponent(params.dagId);
  const runId = decodeURIComponent(params.runId);

  const [run, setRun] = useState<PipelineRun | null>(null);
  const [tasks, setTasks] = useState<TaskInstanceRow[]>([]);
  const [selectedTask, setSelectedTask] = useState<TaskInstanceRow | null>(null);
  const [log, setLog] = useState<TaskLogResponse | null>(null);
  const [logLoading, setLogLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadRun = useCallback(async () => {
    setError(null);
    try {
      const [r, ts] = await Promise.all([
        api.get<PipelineRun>(`/pipelines/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}`),
        api.get<TaskInstanceRow[]>(`/pipelines/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`),
      ]);
      setRun(r);
      setTasks(ts);
      if (!selectedTask && ts.length > 0) {
        // Default selection: the most recently ended task, else the first.
        const ended = ts.filter((t) => t.end_date).sort((a, b) =>
          (b.end_date ?? "").localeCompare(a.end_date ?? ""),
        );
        setSelectedTask(ended[0] ?? ts[0]);
      }
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to load run.");
    } finally {
      setLoading(false);
    }
  }, [dagId, runId, selectedTask]);

  // Initial load + poll every 5s while the run is active.
  useEffect(() => {
    void loadRun();
  }, [loadRun]);

  useEffect(() => {
    if (run?.state === "running" || run?.state === "queued") {
      pollRef.current = setInterval(() => void loadRun(), 5_000);
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = null;
    };
  }, [run?.state, loadRun]);

  // Load the log whenever the selected task changes (or its try_number bumps).
  useEffect(() => {
    if (!selectedTask) {
      setLog(null);
      return;
    }
    let cancelled = false;
    setLogLoading(true);
    api
      .get<TaskLogResponse>(
        `/pipelines/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(selectedTask.task_id)}/logs?try_number=${selectedTask.try_number}`,
      )
      .then((res) => {
        if (!cancelled) setLog(res);
      })
      .catch(() => {
        if (!cancelled) setLog(null);
      })
      .finally(() => {
        if (!cancelled) setLogLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedTask, dagId, runId]);

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <header className="flex flex-wrap items-center gap-2">
        <Link
          href="/pipelines"
          className="inline-flex items-center gap-1 text-xs text-text-secondary hover:text-text-primary"
        >
          <ChevronLeft className="h-3.5 w-3.5" aria-hidden="true" />
          Back to pipelines
        </Link>
        <span className="font-mono text-xs text-text-secondary">{dagId}</span>
        {run && (
          <>
            <RunStateBadge state={run.state} />
            <span className="text-[11px] text-text-secondary">
              {run.duration_sec != null && `${formatMs(Math.round(run.duration_sec * 1000))} - `}
              {run.start_date && formatRelative(run.start_date)}
            </span>
          </>
        )}
        <span className="ml-auto font-mono text-[10px] text-text-secondary">{runId}</span>
      </header>

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Could not load run</span>
            <button
              type="button"
              onClick={() => void loadRun()}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {loading && !run ? (
        <div className="rounded-lg border border-border bg-surface p-8 text-center text-sm text-text-secondary shadow-card">
          Loading run...
        </div>
      ) : (
        <>
          <section>
            <p className="mb-2 text-[11px] uppercase tracking-wide text-text-secondary">
              Task instances
            </p>
            <TaskInstanceList
              tasks={tasks}
              selectedTaskId={selectedTask?.task_id ?? null}
              selectedTry={selectedTask?.try_number ?? null}
              onSelect={setSelectedTask}
            />
          </section>

          <section>
            <p className="mb-2 text-[11px] uppercase tracking-wide text-text-secondary">
              Log
              {selectedTask && (
                <span className="ml-2 font-mono text-text-primary normal-case">
                  {selectedTask.task_id} - try {selectedTask.try_number}
                </span>
              )}
            </p>
            {selectedTask ? (
              <LogViewer
                content={log?.content ?? ""}
                truncated={log?.truncated ?? false}
                sizeBytes={log?.size_bytes ?? 0}
                loading={logLoading}
              />
            ) : (
              <p className="rounded-md border border-border bg-surface p-4 text-xs text-text-secondary">
                Select a task above to view its log.
              </p>
            )}
          </section>
        </>
      )}
    </div>
  );
}
