"use client";

import { useId } from "react";
import { Search, Star } from "lucide-react";
import { cn } from "@/lib/utils";
import type { RunStatus } from "@/lib/types";

export interface HistoryFilterState {
  from: string;
  to: string;
  statuses: Set<RunStatus>;
  favoritesOnly: boolean;
  q: string;
}

interface Props {
  value: HistoryFilterState;
  onChange: (next: HistoryFilterState) => void;
}

const STATUS_OPTIONS: RunStatus[] = ["success", "failed", "stopped", "blocked", "running"];

export function HistoryFilters({ value, onChange }: Props) {
  const fromId = useId();
  const toId = useId();
  const qId = useId();

  function patch(p: Partial<HistoryFilterState>) {
    onChange({ ...value, ...p });
  }

  function toggleStatus(s: RunStatus) {
    const next = new Set(value.statuses);
    if (next.has(s)) next.delete(s);
    else next.add(s);
    patch({ statuses: next });
  }

  return (
    <div className="grid grid-cols-1 gap-3 rounded-lg border border-border bg-surface p-3 shadow-card md:grid-cols-[auto_auto_1fr_auto] md:items-end">
      <div className="flex items-end gap-2">
        <div>
          <label htmlFor={fromId} className="block text-[11px] text-text-secondary">
            From
          </label>
          <input
            id={fromId}
            type="date"
            value={value.from}
            onChange={(e) => patch({ from: e.target.value })}
            className="block rounded-md border border-border bg-background px-2 py-1 text-xs text-text-primary"
          />
        </div>
        <div>
          <label htmlFor={toId} className="block text-[11px] text-text-secondary">
            To
          </label>
          <input
            id={toId}
            type="date"
            value={value.to}
            onChange={(e) => patch({ to: e.target.value })}
            className="block rounded-md border border-border bg-background px-2 py-1 text-xs text-text-primary"
          />
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-1">
        {STATUS_OPTIONS.map((s) => {
          const active = value.statuses.has(s);
          return (
            <button
              key={s}
              type="button"
              onClick={() => toggleStatus(s)}
              aria-pressed={active}
              className={cn(
                "rounded-full border px-2.5 py-1 text-[11px] capitalize",
                active
                  ? "border-accent/40 bg-accent/10 text-accent"
                  : "border-border text-text-secondary hover:text-text-primary",
              )}
            >
              {s}
            </button>
          );
        })}
      </div>

      <div className="flex items-end gap-2">
        <div className="flex-1">
          <label htmlFor={qId} className="block text-[11px] text-text-secondary">
            Search question
          </label>
          <div className="relative">
            <Search
              className="pointer-events-none absolute left-2 top-1.5 h-3.5 w-3.5 text-text-secondary"
              aria-hidden="true"
            />
            <input
              id={qId}
              type="search"
              value={value.q}
              onChange={(e) => patch({ q: e.target.value })}
              placeholder="doanh thu, brand, ..."
              className="block w-full rounded-md border border-border bg-background pl-7 pr-2 py-1 text-xs text-text-primary"
            />
          </div>
        </div>
      </div>

      <label
        className={cn(
          "inline-flex cursor-pointer items-center gap-1 self-end rounded-md border border-border bg-surface px-3 py-1.5 text-xs",
          value.favoritesOnly ? "border-warning/40 text-warning" : "text-text-secondary",
        )}
      >
        <input
          type="checkbox"
          checked={value.favoritesOnly}
          onChange={(e) => patch({ favoritesOnly: e.target.checked })}
          className="sr-only"
        />
        <Star
          className={cn("h-3.5 w-3.5", value.favoritesOnly && "fill-current")}
          aria-hidden="true"
        />
        Favorites only
      </label>
    </div>
  );
}
