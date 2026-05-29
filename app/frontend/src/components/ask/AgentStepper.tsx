"use client";

import {
  CheckCircle2,
  CircleDashed,
  Circle,
  Database,
  FileCode,
  Loader2,
  Play,
  ShieldCheck,
  Sparkles,
  XCircle,
  type LucideIcon,
} from "lucide-react";
import type { AgentStepName, AgentStepStatus } from "@/lib/types";
import { cn } from "@/lib/utils";

interface Props {
  steps: Record<AgentStepName, AgentStepStatus>;
}

const STEP_META: Array<{
  name: AgentStepName;
  label: string;
  icon: LucideIcon;
}> = [
  { name: "load_metadata", label: "Load metadata", icon: Database },
  { name: "build_prompt", label: "Build prompt", icon: FileCode },
  { name: "generate_sql", label: "Generate SQL", icon: FileCode },
  { name: "guard_sql", label: "Validate SQL", icon: ShieldCheck },
  { name: "execute_sql", label: "Execute", icon: Play },
  { name: "summarize", label: "Summarize", icon: Sparkles },
];

function statusIcon(status: AgentStepStatus) {
  if (status === "ok") return CheckCircle2;
  if (status === "running") return Loader2;
  if (status === "error") return XCircle;
  if (status === "cancelled") return CircleDashed;
  return Circle;
}

function statusColor(status: AgentStepStatus) {
  if (status === "ok") return "text-success";
  if (status === "running") return "text-accent";
  if (status === "error") return "text-error";
  if (status === "cancelled") return "text-warning";
  return "text-text-secondary";
}

export function AgentStepper({ steps }: Props) {
  return (
    <ol className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-surface p-3 shadow-card md:flex-nowrap">
      {STEP_META.map(({ name, label, icon: Icon }, idx) => {
        const status = steps[name];
        const StatusIcon = statusIcon(status);
        const color = statusColor(status);
        return (
          <li
            key={name}
            className="flex flex-1 items-center gap-2"
            aria-label={`${label}: ${status}`}
          >
            <span
              className={cn(
                "inline-flex h-6 w-6 items-center justify-center rounded-full border border-border bg-background",
                status === "running" && "border-accent/50",
                status === "ok" && "border-success/40 bg-success/10",
                status === "error" && "border-error/40 bg-error/10",
                status === "cancelled" && "border-warning/40 bg-warning/10",
              )}
            >
              <Icon className="h-3.5 w-3.5 text-text-secondary" aria-hidden="true" />
            </span>
            <span className={cn("flex items-center gap-1 text-xs", color)}>
              <StatusIcon
                className={cn("h-3.5 w-3.5", status === "running" && "animate-spin")}
                aria-hidden="true"
              />
              <span className="hidden text-text-primary md:inline">{label}</span>
            </span>
            {idx < STEP_META.length - 1 && (
              <span aria-hidden="true" className="hidden h-px flex-1 bg-border md:block" />
            )}
          </li>
        );
      })}
    </ol>
  );
}
