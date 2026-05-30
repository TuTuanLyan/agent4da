"use client";

import { useState } from "react";
import { BarChart3, FileCode, MessageSquareText, Table2 } from "lucide-react";
import { AnswerPanel } from "./AnswerPanel";
import { ChartPanel } from "./ChartPanel";
import { SqlPanel } from "./SqlPanel";
import { TablePanel } from "./TablePanel";
import type { AskResult } from "@/lib/types";
import { cn } from "@/lib/utils";

type Tab = "answer" | "chart" | "table" | "sql";

const TABS: Array<{ key: Tab; label: string; icon: typeof BarChart3 }> = [
  { key: "answer", label: "Answer", icon: MessageSquareText },
  { key: "chart", label: "Chart", icon: BarChart3 },
  { key: "table", label: "Table", icon: Table2 },
  { key: "sql", label: "SQL", icon: FileCode },
];

interface Props {
  result: AskResult;
}

export function ResultTabs({ result }: Props) {
  const [tab, setTab] = useState<Tab>("answer");

  return (
    <section className="rounded-lg border border-border bg-surface shadow-card">
      <div role="tablist" className="flex items-center gap-1 border-b border-border px-2 pt-2">
        {TABS.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            type="button"
            role="tab"
            aria-selected={tab === key}
            onClick={() => setTab(key)}
            className={cn(
              "inline-flex items-center gap-1 rounded-t-md px-3 py-1.5 text-xs",
              tab === key
                ? "border border-b-0 border-border bg-background text-text-primary"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            <Icon className="h-3.5 w-3.5" aria-hidden="true" />
            {label}
          </button>
        ))}
        <span className="ml-auto pb-2 pr-2 text-[11px] text-text-secondary">
          {result.row_count.toLocaleString()} rows
          {result.latency_ms != null && ` - ${result.latency_ms} ms`}
        </span>
      </div>

      <div className="p-4">
        {tab === "answer" && (
          <AnswerPanel summary={result.summary} keyNumbers={result.key_numbers} />
        )}
        {tab === "chart" && (
          <ChartPanel
            columns={result.columns}
            rows={result.rows}
            suggestion={result.chart_suggestion}
          />
        )}
        {tab === "table" && <TablePanel columns={result.columns} rows={result.rows} />}
        {tab === "sql" && (
          <SqlPanel sql={result.generated_sql} guardStatus={result.guard_status} />
        )}
      </div>
    </section>
  );
}
