/** Shared types matching the FastAPI response shapes. */

export type AgentStepName =
  | "load_metadata"
  | "build_prompt"
  | "generate_sql"
  | "guard_sql"
  | "execute_sql"
  | "summarize";

export type AgentStepStatus = "pending" | "running" | "ok" | "error" | "cancelled";

export type ChartType = "auto" | "bar" | "line" | "pie" | "table" | "scatter";

export interface ChartSuggestion {
  chart_type: "bar" | "line" | "pie" | "scatter";
  x: string;
  y: string;
  series?: string[];
  sort?: string;
}

export interface AskKeyNumber {
  label: string;
  value: unknown;
  delta?: string | null;
}

export type RunStatus = "running" | "success" | "failed" | "stopped" | "blocked";

export interface AskResult {
  run_id: string;
  question: string;
  generated_sql: string | null;
  guard_status: string | null;
  columns: string[];
  rows: Array<Record<string, unknown>>;
  row_count: number;
  error: string | null;
  latency_ms: number | null;
  summary: string | null;
  key_numbers: AskKeyNumber[];
  chart_suggestion: ChartSuggestion | null;
  status: RunStatus;
  created_at: string;
}

export interface SampleQuestion {
  id: string;
  label: string;
  question: string;
  sort_order: number;
}
