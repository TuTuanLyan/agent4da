"use client";

import { RunStateBadge } from "./RunStateBadge";
import { formatMs, formatRelative } from "@/lib/format";
import { cn } from "@/lib/utils";

export interface TaskInstanceRow {
  task_id: string;
  state: string | null;
  try_number: number;
  max_tries: number;
  start_date: string | null;
  end_date: string | null;
  duration_sec: number | null;
  operator: string | null;
}

interface Props {
  tasks: TaskInstanceRow[];
  selectedTaskId: string | null;
  selectedTry: number | null;
  onSelect: (task: TaskInstanceRow) => void;
}

export function TaskInstanceList({ tasks, selectedTaskId, selectedTry, onSelect }: Props) {
  if (!tasks.length) {
    return (
      <p className="rounded-md border border-border bg-surface p-4 text-xs text-text-secondary">
        No task instances yet.
      </p>
    );
  }
  return (
    <ul className="divide-y divide-border overflow-hidden rounded-lg border border-border bg-surface text-xs">
      {tasks.map((t) => {
        const active = t.task_id === selectedTaskId && t.try_number === selectedTry;
        return (
          <li key={`${t.task_id}-${t.try_number}`}>
            <button
              type="button"
              onClick={() => onSelect(t)}
              className={cn(
                "flex w-full items-center gap-3 px-3 py-2 text-left",
                active ? "bg-accent/10" : "hover:bg-background",
              )}
            >
              <RunStateBadge state={t.state} />
              <span className="min-w-0 flex-1 truncate font-mono text-text-primary">
                {t.task_id}
              </span>
              <span className="hidden text-text-secondary sm:inline">
                {t.operator ?? ""}
              </span>
              <span className="tabular-nums text-text-secondary">
                try {t.try_number}/{Math.max(t.max_tries, t.try_number)}
              </span>
              <span className="tabular-nums text-text-secondary">
                {t.duration_sec != null
                  ? formatMs(Math.round(t.duration_sec * 1000))
                  : "-"}
              </span>
              <span className="text-text-secondary">
                {t.end_date ? formatRelative(t.end_date) : "-"}
              </span>
            </button>
          </li>
        );
      })}
    </ul>
  );
}
