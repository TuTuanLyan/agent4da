import { cn } from "@/lib/utils";

export type TableKind = "fact" | "dimension" | "summary" | "semantic";

const META: Record<TableKind, { label: string; cls: string }> = {
  fact: {
    label: "fact",
    cls: "border-accent-2/40 bg-accent-2/10 text-accent-2",
  },
  dimension: {
    label: "dim",
    cls: "border-accent/40 bg-accent/10 text-accent",
  },
  summary: {
    label: "summary",
    // Violet tone via inline hex - kept literal so it survives theme toggles.
    cls: "border-[#8B5CF6]/40 bg-[#8B5CF6]/10 text-[#8B5CF6]",
  },
  semantic: {
    label: "semantic",
    cls: "border-border bg-surface text-text-secondary",
  },
};

export function KindBadge({ kind }: { kind: TableKind }) {
  const m = META[kind] ?? META.semantic;
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md border px-2 py-0.5 text-[11px] font-medium",
        m.cls,
      )}
    >
      {m.label}
    </span>
  );
}
