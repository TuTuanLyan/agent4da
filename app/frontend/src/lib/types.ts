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
export type AnswerType = "answer" | "clarification" | "empty_result" | "blocked" | "metadata";

export interface ClarificationSuggestion {
  label: string;
  question: string;
  reason: string;
  intent: string;
  confidence: "low" | "medium" | "high";
}

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
  answer?: string | null;
  insights: string[];
  key_numbers: AskKeyNumber[];
  chart_suggestion: ChartSuggestion | null;
  chart_type?: string | null;
  chart?: Record<string, unknown> | null;
  chart_data?: Array<Record<string, unknown>>;
  agent_engine: string;
  status: RunStatus;
  session_id: string | null;
  turn_index: number | null;
  answer_type?: AnswerType;
  needs_clarification?: boolean;
  clarification_suggestions?: ClarificationSuggestion[];
  assumptions?: string[];
  retry_count?: number | null;
  model_used?: string | null;
  intent?: string | null;
  used_tables?: string[];
  warnings?: string[];
  validation_notes?: string[];
  confidence?: string | null;
  context_used?: boolean;
  resolved_question?: string | null;
  created_at: string;
}

export interface SampleQuestion {
  id: string;
  label: string;
  question: string;
  sort_order: number;
}

/** A chat thread, as returned by POST /agent/sessions. */
export interface ChatSession {
  id: string;
  title: string | null;
  is_pinned: boolean;
  pinned_at: string | null;
  created_at: string;
  last_used_at: string;
}

/** List item for the chat sidebar (GET /agent/sessions). */
export interface ChatSessionSummary extends ChatSession {
  run_count: number;
  last_question: string | null;
  last_status: RunStatus | null;
}
