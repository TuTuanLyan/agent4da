"use client";

import { useCallback, useRef, useState } from "react";
import { API_BASE_URL } from "@/lib/api";
import { getAccessToken } from "@/lib/auth";
import type {
  AgentStepName,
  AgentStepStatus,
  AskResult,
} from "@/lib/types";

interface StepEvent {
  step: string;
  status: AgentStepStatus | "ok" | "cancelled" | string;
  run_id?: string;
  error?: string;
}

const KNOWN_STEPS: AgentStepName[] = [
  "load_metadata",
  "build_prompt",
  "generate_sql",
  "guard_sql",
  "execute_sql",
  "summarize",
];

export type StepMap = Record<AgentStepName, AgentStepStatus>;

function initialSteps(): StepMap {
  return KNOWN_STEPS.reduce((acc, s) => {
    acc[s] = "pending";
    return acc;
  }, {} as StepMap);
}

interface UseAgentStream {
  steps: StepMap;
  result: AskResult | null;
  runId: string | null;
  error: string | null;
  streaming: boolean;
  start: (question: string, opts?: { summarize?: boolean; chartType?: string }) => Promise<void>;
  stop: () => Promise<void>;
  reset: () => void;
}

/** Drives the SSE stream from GET /agent/stream.
 *  - We use fetch + ReadableStream instead of EventSource so we can send the
 *    Authorization header.
 *  - AbortController is owned by us; stop() aborts the stream AND fires
 *    POST /agent/stop so the backend cancels the running task. */
export function useAgentStream(): UseAgentStream {
  const [steps, setSteps] = useState<StepMap>(initialSteps);
  const [result, setResult] = useState<AskResult | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [streaming, setStreaming] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const runIdRef = useRef<string | null>(null);

  const reset = useCallback(() => {
    setSteps(initialSteps());
    setResult(null);
    setRunId(null);
    setError(null);
    setStreaming(false);
    runIdRef.current = null;
    abortRef.current = null;
  }, []);

  const handleStep = useCallback((evt: StepEvent) => {
    if (evt.step === "starting" && evt.run_id) {
      setRunId(evt.run_id);
      runIdRef.current = evt.run_id;
      return;
    }
    if (evt.step === "error") {
      setError(evt.error ?? "Unknown error");
      return;
    }
    if (evt.step === "stopped") {
      setSteps((s) => {
        const next = { ...s };
        for (const k of KNOWN_STEPS) {
          if (next[k] === "running") next[k] = "cancelled";
        }
        return next;
      });
      return;
    }
    if (KNOWN_STEPS.includes(evt.step as AgentStepName)) {
      const name = evt.step as AgentStepName;
      const status: AgentStepStatus =
        evt.status === "ok"
          ? "ok"
          : evt.status === "running"
            ? "running"
            : evt.status === "error"
              ? "error"
              : evt.status === "cancelled"
                ? "cancelled"
                : "pending";
      setSteps((s) => ({ ...s, [name]: status }));
    }
  }, []);

  const start = useCallback(
    async (question: string, opts?: { summarize?: boolean; chartType?: string }) => {
      reset();
      setStreaming(true);

      const ctrl = new AbortController();
      abortRef.current = ctrl;

      const params = new URLSearchParams({ question });
      if (opts?.summarize !== undefined) params.set("summarize", String(opts.summarize));
      if (opts?.chartType) params.set("chart_type", opts.chartType);

      const token = getAccessToken();
      let res: Response;
      try {
        res = await fetch(`${API_BASE_URL}/agent/stream?${params.toString()}`, {
          method: "GET",
          headers: {
            Authorization: token ? `Bearer ${token}` : "",
            Accept: "text/event-stream",
          },
          credentials: "include",
          signal: ctrl.signal,
        });
      } catch (err) {
        if ((err as Error).name === "AbortError") {
          setStreaming(false);
          return;
        }
        setError((err as Error).message);
        setStreaming(false);
        return;
      }

      if (!res.ok || !res.body) {
        let detail = `Stream failed: HTTP ${res.status}`;
        try {
          const body = await res.json();
          if (body?.detail) detail = String(body.detail);
        } catch {
          /* keep default */
        }
        setError(detail);
        setStreaming(false);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          // SSE events are separated by a blank line.
          let sep: number;
          while ((sep = buffer.indexOf("\n\n")) !== -1) {
            const chunk = buffer.slice(0, sep);
            buffer = buffer.slice(sep + 2);
            const lines = chunk.split("\n");
            let event = "message";
            const dataLines: string[] = [];
            for (const line of lines) {
              if (line.startsWith("event:")) event = line.slice(6).trim();
              else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
            }
            const dataStr = dataLines.join("\n");
            if (!dataStr) continue;
            let payload: Record<string, unknown>;
            try {
              payload = JSON.parse(dataStr);
            } catch {
              continue;
            }
            if (event === "step") {
              handleStep(payload as unknown as StepEvent);
            } else if (event === "result") {
              setResult(payload as unknown as AskResult);
              setRunId((payload as { run_id: string }).run_id);
              runIdRef.current = (payload as { run_id: string }).run_id;
            }
          }
        }
      } catch (err) {
        if ((err as Error).name !== "AbortError") {
          setError((err as Error).message);
        }
      } finally {
        setStreaming(false);
      }
    },
    [handleStep, reset],
  );

  const stop = useCallback(async () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    const id = runIdRef.current;
    if (id) {
      try {
        const token = getAccessToken();
        await fetch(`${API_BASE_URL}/agent/stop`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: token ? `Bearer ${token}` : "",
          },
          credentials: "include",
          body: JSON.stringify({ run_id: id }),
        });
      } catch {
        /* best effort */
      }
    }
    setStreaming(false);
  }, []);

  return { steps, result, runId, error, streaming, start, stop, reset };
}
