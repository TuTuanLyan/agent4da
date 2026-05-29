"use client";

import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { ActionBar } from "@/components/ask/ActionBar";
import { AgentStepper } from "@/components/ask/AgentStepper";
import { QuestionInput } from "@/components/ask/QuestionInput";
import { QuickStatsStrip } from "@/components/ask/QuickStatsStrip";
import { ResultTabs } from "@/components/ask/ResultTabs";
import { SampleChips } from "@/components/ask/SampleChips";
import { useAgentStream } from "@/hooks/useAgentStream";

export default function AskPage() {
  const { steps, result, error, streaming, start, stop } = useAgentStream();
  const [draft, setDraft] = useState("");
  const params = useSearchParams();

  // Prefill the input when navigated from "Re-run this question" on history.
  useEffect(() => {
    const q = params.get("question");
    if (q && !draft) setDraft(q);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [params]);

  const onRun = useCallback(
    (question?: string) => {
      const q = (question ?? draft).trim();
      if (!q) return;
      setDraft(q);
      void start(q);
    },
    [draft, start],
  );

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <QuickStatsStrip />

      <QuestionInput
        value={draft}
        onChange={setDraft}
        streaming={streaming}
        onRun={(q) => {
          setDraft(q);
          onRun(q);
        }}
        onStop={stop}
      />

      <SampleChips
        onPick={(q) => {
          setDraft(q);
        }}
      />

      <AgentStepper steps={steps} />

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Something went wrong</span>
            <button
              type="button"
              onClick={() => onRun(draft)}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      {!result && !streaming && !error && (
        <section className="rounded-lg border border-border bg-surface p-6 text-sm text-text-secondary shadow-card">
          Ask a question above to see the agent run. Try one of the sample
          chips, or type your own in Vietnamese or English.
        </section>
      )}

      {streaming && !result && (
        <section className="rounded-lg border border-border bg-surface p-6 shadow-card">
          <div className="flex items-center gap-3">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-accent" aria-hidden="true" />
            <p className="text-sm text-text-secondary">
              Running the agent... live updates above.
            </p>
          </div>
        </section>
      )}

      {result && (
        <>
          <ResultTabs result={result} />
          <ActionBar
            result={result}
            streaming={streaming}
            canRun={draft.trim().length > 0 && !streaming}
            onRun={() => onRun(draft)}
            onStop={stop}
          />
        </>
      )}
    </div>
  );
}
