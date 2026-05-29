import { cn } from "@/lib/utils";

export type StatusValue =
  | "ok"
  | "degraded"
  | "down"
  | "configured"
  | "missing"
  | "unknown";

const VARIANTS: Record<StatusValue, { dotClass: string; label: string }> = {
  ok: { dotClass: "bg-success", label: "OK" },
  degraded: { dotClass: "bg-warning", label: "Degraded" },
  down: { dotClass: "bg-error", label: "Down" },
  configured: { dotClass: "bg-success", label: "Configured" },
  missing: { dotClass: "bg-error", label: "Missing" },
  unknown: { dotClass: "bg-text-secondary/50", label: "Unknown" },
};

export interface StatusPillProps {
  label: string;
  status?: StatusValue;
  detail?: string | null;
  latencyMs?: number | null;
  className?: string;
}

/** Topbar service health pill: colored dot + label.
 *  Status is fed by useHealth() polling /ops/health (Phase 7). */
export function StatusPill({
  label,
  status = "unknown",
  detail,
  latencyMs,
  className,
}: StatusPillProps) {
  const variant = VARIANTS[status];
  const titleParts: string[] = [`${label}: ${variant.label}`];
  if (latencyMs != null) titleParts.push(`${latencyMs} ms`);
  if (detail) titleParts.push(detail);
  return (
    <span
      role="status"
      aria-label={`${label}: ${variant.label}`}
      title={titleParts.join(" - ")}
      className={cn(
        "inline-flex items-center gap-2 rounded-md border border-border bg-surface px-2.5 py-1 text-xs font-medium text-text-secondary",
        className,
      )}
    >
      <span
        aria-hidden="true"
        className={cn("h-2 w-2 rounded-full", variant.dotClass)}
      />
      <span className="text-text-primary">{label}</span>
    </span>
  );
}
