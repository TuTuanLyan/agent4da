import type { RunStatus } from "@/lib/types";
import { cn } from "@/lib/utils";

const META: Record<
  RunStatus,
  { label: string; tone: "success" | "error" | "warning" | "secondary" }
> = {
  success: { label: "Success", tone: "success" },
  failed: { label: "Failed", tone: "error" },
  stopped: { label: "Stopped", tone: "warning" },
  blocked: { label: "Blocked", tone: "error" },
  running: { label: "Running", tone: "secondary" },
};

const TONE_CLS: Record<string, string> = {
  success: "border-success/40 bg-success/10 text-success",
  error: "border-error/40 bg-error/10 text-error",
  warning: "border-warning/40 bg-warning/10 text-warning",
  secondary: "border-border bg-surface text-text-secondary",
};

export function StatusBadge({ status }: { status: RunStatus }) {
  const m = META[status] ?? META.success;
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md border px-2 py-0.5 text-[11px] font-medium",
        TONE_CLS[m.tone],
      )}
    >
      {m.label}
    </span>
  );
}
