"use client";

import { StatusPill } from "./StatusPill";
import { ThemeToggle } from "./ThemeToggle";
import { UserMenu } from "@/components/auth/UserMenu";
import { useHealth, type ServiceSnapshot } from "@/hooks/useHealth";

/** Top bar: project name on the left; service status pills, theme toggle,
 *  and user avatar on the right. Pills update every 30s via useHealth(). */
export function Topbar() {
  const { data } = useHealth();

  return (
    <header className="flex h-14 items-center justify-between border-b border-border bg-surface px-4">
      <div className="flex items-center gap-2">
        <span className="text-sm font-semibold text-text-primary">
          Analytics Console
        </span>
        <span className="rounded-md border border-border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-text-secondary">
          Agent4DA
        </span>
      </div>

      <div className="flex items-center gap-3">
        <div className="hidden items-center gap-2 sm:flex">
          <Pill label="Trino" snapshot={data.trino} />
          <Pill label="Spark" snapshot={data.spark} />
          <Pill label="Airflow" snapshot={data.airflow} />
        </div>

        <ThemeToggle />

        <UserMenu />
      </div>
    </header>
  );
}

function Pill({ label, snapshot }: { label: string; snapshot: ServiceSnapshot }) {
  return (
    <StatusPill
      label={label}
      status={snapshot.status}
      detail={snapshot.detail}
      latencyMs={snapshot.latency_ms}
    />
  );
}
