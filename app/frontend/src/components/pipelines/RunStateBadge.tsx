import { cn } from "@/lib/utils";

const TONE: Record<string, string> = {
  success: "border-success/40 bg-success/10 text-success",
  failed: "border-error/40 bg-error/10 text-error",
  upstream_failed: "border-error/40 bg-error/10 text-error",
  running: "border-accent/40 bg-accent/10 text-accent",
  queued: "border-accent/40 bg-accent/10 text-accent",
  scheduled: "border-border bg-surface text-text-secondary",
  up_for_retry: "border-warning/40 bg-warning/10 text-warning",
  up_for_reschedule: "border-warning/40 bg-warning/10 text-warning",
  skipped: "border-border bg-surface text-text-secondary",
};

export function RunStateBadge({ state }: { state: string | null | undefined }) {
  const s = (state ?? "").toLowerCase();
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md border px-2 py-0.5 text-[11px] font-medium",
        TONE[s] ?? "border-border bg-surface text-text-secondary",
      )}
    >
      {state ?? "-"}
    </span>
  );
}
