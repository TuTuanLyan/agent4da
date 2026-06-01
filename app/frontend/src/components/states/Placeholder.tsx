import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/** Generic placeholder card used by Phase 1 routes that don't have their
 *  feature wired yet. Replace per-phase with the real Empty / Loading /
 *  Error variants from src/components/states/ (Phase 3+). */
export function Placeholder({
  title,
  body,
  className,
  children,
}: {
  title: string;
  body?: string;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <section
      className={cn(
        "rounded-lg border border-border bg-surface p-6 shadow-card",
        className,
      )}
    >
      <h2 className="text-base font-semibold text-text-primary">{title}</h2>
      {body && <p className="mt-1 text-sm text-text-secondary">{body}</p>}
      {children && <div className="mt-4">{children}</div>}
    </section>
  );
}
