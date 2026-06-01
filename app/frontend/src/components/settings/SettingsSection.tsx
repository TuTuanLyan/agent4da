import type { ReactNode } from "react";

interface Props {
  title: string;
  description?: string;
  children: ReactNode;
}

/** Generic card wrapping each settings group. */
export function SettingsSection({ title, description, children }: Props) {
  return (
    <section className="rounded-lg border border-border bg-surface p-4 shadow-card">
      <header>
        <h2 className="text-sm font-semibold text-text-primary">{title}</h2>
        {description && (
          <p className="mt-0.5 text-xs text-text-secondary">{description}</p>
        )}
      </header>
      <div className="mt-3">{children}</div>
    </section>
  );
}
