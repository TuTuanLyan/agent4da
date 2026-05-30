"use client";

import { useState } from "react";
import { Check, Copy, ShieldCheck, ShieldX, ShieldAlert } from "lucide-react";
import { cn } from "@/lib/utils";

interface Props {
  sql: string | null;
  guardStatus: string | null;
}

function guardMeta(status: string | null) {
  if (!status) return { label: "No SQL", icon: ShieldAlert, tone: "warning" as const };
  if (status === "pass") return { label: "PASS", icon: ShieldCheck, tone: "success" as const };
  if (status === "auto_limited")
    return { label: "AUTO-LIMITED", icon: ShieldAlert, tone: "warning" as const };
  if (status === "blocked")
    return { label: "BLOCKED", icon: ShieldX, tone: "error" as const };
  return { label: status.toUpperCase(), icon: ShieldAlert, tone: "warning" as const };
}

export function SqlPanel({ sql, guardStatus }: Props) {
  const [copied, setCopied] = useState(false);
  const meta = guardMeta(guardStatus);
  const ToneIcon = meta.icon;

  async function onCopy() {
    if (!sql) return;
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard might be blocked; ignore */
    }
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <span
          className={cn(
            "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[11px] font-medium",
            meta.tone === "success" && "border-success/40 bg-success/10 text-success",
            meta.tone === "warning" && "border-warning/40 bg-warning/10 text-warning",
            meta.tone === "error" && "border-error/40 bg-error/10 text-error",
          )}
        >
          <ToneIcon className="h-3 w-3" aria-hidden="true" />
          Guard: {meta.label}
        </span>
        <span className="ml-auto" />
        <button
          type="button"
          onClick={onCopy}
          disabled={!sql}
          className={cn(
            "inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-text-secondary",
            "hover:text-text-primary disabled:opacity-50",
          )}
        >
          {copied ? (
            <>
              <Check className="h-3 w-3 text-success" aria-hidden="true" /> Copied
            </>
          ) : (
            <>
              <Copy className="h-3 w-3" aria-hidden="true" /> Copy
            </>
          )}
        </button>
      </div>

      {sql ? (
        <pre className="overflow-x-auto rounded-md border border-border bg-background p-3 font-mono text-xs leading-relaxed text-text-primary">
          {sql}
        </pre>
      ) : (
        <p className="text-sm text-text-secondary">No SQL generated yet.</p>
      )}
    </div>
  );
}
