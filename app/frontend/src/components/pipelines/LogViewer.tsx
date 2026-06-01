"use client";

import { useEffect, useRef, useState } from "react";
import { Check, Copy, MoveVertical } from "lucide-react";
import { cn } from "@/lib/utils";

interface Props {
  content: string;
  truncated: boolean;
  sizeBytes: number;
  loading: boolean;
}

export function LogViewer({ content, truncated, sizeBytes, loading }: Props) {
  const [wrap, setWrap] = useState(false);
  const [follow, setFollow] = useState(true);
  const [copied, setCopied] = useState(false);
  const preRef = useRef<HTMLPreElement>(null);

  useEffect(() => {
    if (follow && preRef.current) {
      preRef.current.scrollTop = preRef.current.scrollHeight;
    }
  }, [content, follow]);

  async function copyAll() {
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard may be blocked */
    }
  }

  return (
    <div className="flex flex-col overflow-hidden rounded-lg border border-border bg-surface shadow-card">
      <div className="flex flex-wrap items-center gap-2 border-b border-border px-3 py-2 text-xs">
        <span className="text-text-secondary">
          {loading ? "Loading..." : truncated ? "Tail shown" : "Full log"}
          {" - "}
          {(sizeBytes / 1024).toFixed(1)} KB
        </span>
        <span className="ml-auto" />
        <label className="inline-flex items-center gap-1 text-text-secondary">
          <input
            type="checkbox"
            checked={wrap}
            onChange={(e) => setWrap(e.target.checked)}
          />
          Wrap
        </label>
        <label className="inline-flex items-center gap-1 text-text-secondary">
          <input
            type="checkbox"
            checked={follow}
            onChange={(e) => setFollow(e.target.checked)}
          />
          <MoveVertical className="h-3 w-3" aria-hidden="true" />
          Follow
        </label>
        <button
          type="button"
          onClick={copyAll}
          className="inline-flex items-center gap-1 rounded border border-border bg-surface px-2 py-0.5 text-text-secondary hover:text-text-primary"
        >
          {copied ? (
            <>
              <Check className="h-3 w-3 text-success" aria-hidden="true" /> Copied
            </>
          ) : (
            <>
              <Copy className="h-3 w-3" aria-hidden="true" /> Copy all
            </>
          )}
        </button>
      </div>
      <pre
        ref={preRef}
        className={cn(
          "h-[60vh] overflow-auto bg-background p-3 font-mono text-[11px] leading-relaxed text-text-primary",
          wrap ? "whitespace-pre-wrap break-words" : "whitespace-pre",
        )}
      >
        {content || (loading ? "Loading log..." : "(empty log)")}
      </pre>
    </div>
  );
}
