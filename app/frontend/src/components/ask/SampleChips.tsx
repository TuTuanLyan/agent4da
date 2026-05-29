"use client";

import { useEffect, useState } from "react";
import { Sparkles } from "lucide-react";
import { api } from "@/lib/api";
import type { SampleQuestion } from "@/lib/types";
import { cn } from "@/lib/utils";

interface Props {
  onPick: (question: string) => void;
}

export function SampleChips({ onPick }: Props) {
  const [items, setItems] = useState<SampleQuestion[]>([]);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api
      .get<SampleQuestion[]>("/agent/sample-questions")
      .then((value) => {
        if (!cancelled) setItems(value);
      })
      .catch(() => {
        /* sample chips are optional polish */
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (loaded && items.length === 0) return null;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="inline-flex items-center gap-1 text-[11px] text-text-secondary">
        <Sparkles className="h-3 w-3" aria-hidden="true" />
        Try:
      </span>
      {items.map((q) => (
        <button
          key={q.id}
          type="button"
          onClick={() => onPick(q.question)}
          className={cn(
            "rounded-full border border-border bg-surface px-3 py-1 text-xs text-text-secondary",
            "hover:border-accent/40 hover:text-text-primary",
          )}
        >
          {q.label}
        </button>
      ))}
    </div>
  );
}
