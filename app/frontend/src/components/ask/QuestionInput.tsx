"use client";

import { FormEvent, KeyboardEvent, useState } from "react";
import { Send, StopCircle } from "lucide-react";
import { cn } from "@/lib/utils";

interface Props {
  // Controlled value owned by the Ask page so sample chips, the history
  // "Re-run" prefill, and typing all share one source of truth.
  value: string;
  onChange: (value: string) => void;
  streaming: boolean;
  onRun: (question: string) => void;
  onStop: () => void;
}

export function QuestionInput({ value, onChange, streaming, onRun, onStop }: Props) {
  const [touched, setTouched] = useState(false);

  function submit(e?: FormEvent | KeyboardEvent) {
    if (e && "preventDefault" in e) e.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setTouched(true);
      return;
    }
    onRun(trimmed);
  }

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      submit(e);
    }
  }

  return (
    <form
      onSubmit={submit}
      className="rounded-lg border border-border bg-surface p-4 shadow-card"
    >
      <label htmlFor="question" className="block text-xs font-medium text-text-secondary">
        Ask the data (Vietnamese or English)
      </label>
      <textarea
        id="question"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={onKeyDown}
        rows={3}
        placeholder="Doanh thu theo ngay trong thang 1 nam 2020 ..."
        className={cn(
          "mt-1 block w-full resize-y rounded-md border border-border bg-background px-3 py-2 text-sm text-text-primary",
          "placeholder:text-text-secondary focus:border-accent focus:outline-none",
        )}
      />
      <div className="mt-2 flex items-center justify-between">
        <p className="text-[11px] text-text-secondary">
          Cmd / Ctrl + Enter to run.
        </p>
        <div className="flex items-center gap-2">
          {streaming ? (
            <button
              type="button"
              onClick={onStop}
              className={cn(
                "inline-flex items-center gap-1 rounded-md border border-warning/40 bg-warning/10 px-3 py-1.5 text-xs font-medium text-warning",
                "hover:bg-warning/20",
              )}
            >
              <StopCircle className="h-3.5 w-3.5" aria-hidden="true" />
              Stop
            </button>
          ) : (
            <button
              type="submit"
              disabled={streaming || (!value.trim() && touched)}
              className={cn(
                "inline-flex items-center gap-1 rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-white",
                "hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60",
              )}
            >
              <Send className="h-3.5 w-3.5" aria-hidden="true" />
              Run
            </button>
          )}
        </div>
      </div>
    </form>
  );
}
