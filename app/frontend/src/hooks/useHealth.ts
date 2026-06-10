"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "@/lib/api";
import { useAuth } from "@/components/auth/AuthProvider";

export type ServiceStatus =
  | "ok"
  | "degraded"
  | "down"
  | "configured"
  | "missing"
  | "unknown";

export interface ServiceSnapshot {
  status: ServiceStatus;
  version?: string | null;
  workers?: number | null;
  latency_ms?: number | null;
  detail?: string | null;
}

export interface HealthSnapshot {
  trino: ServiceSnapshot;
  spark: ServiceSnapshot;
  airflow: ServiceSnapshot;
  gemini: ServiceSnapshot;
  groq: ServiceSnapshot;
  checked_at: string;
}

const POLL_MS = 30_000;
const UNKNOWN: ServiceSnapshot = { status: "unknown" };

const PLACEHOLDER: HealthSnapshot = {
  trino: UNKNOWN,
  spark: UNKNOWN,
  airflow: UNKNOWN,
  gemini: UNKNOWN,
  groq: UNKNOWN,
  checked_at: new Date(0).toISOString(),
};

/** Polls /ops/health every 30s while the user is signed in and the tab is
 *  visible. Returns the latest snapshot plus a manual refresh handle. */
export function useHealth() {
  const { user } = useAuth();
  const [data, setData] = useState<HealthSnapshot>(PLACEHOLDER);
  const [loading, setLoading] = useState(false);
  const timer = useRef<ReturnType<typeof setInterval> | null>(null);
  const inflightRef = useRef(false);

  const refresh = useCallback(async () => {
    if (!user || inflightRef.current) return;
    inflightRef.current = true;
    setLoading(true);
    try {
      const res = await api.get<HealthSnapshot>("/ops/health");
      setData(res);
    } catch {
      // Keep last good snapshot; surface "unknown" only on first failure.
      setData((prev) =>
        prev.checked_at === PLACEHOLDER.checked_at
          ? PLACEHOLDER
          : prev,
      );
    } finally {
      inflightRef.current = false;
      setLoading(false);
    }
  }, [user]);

  useEffect(() => {
    if (!user) {
      setData(PLACEHOLDER);
      return;
    }
    void refresh();
    timer.current = setInterval(() => void refresh(), POLL_MS);

    function onVisibility() {
      if (document.visibilityState === "visible") {
        void refresh();
      }
    }
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      if (timer.current) clearInterval(timer.current);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [user, refresh]);

  return { data, loading, refresh };
}
