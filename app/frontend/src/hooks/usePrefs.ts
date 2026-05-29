"use client";

import { useCallback, useEffect, useState } from "react";
import { useTheme } from "next-themes";
import { api } from "@/lib/api";
import type { AuthUserPreferences } from "@/lib/auth";
import { useAuth } from "@/components/auth/AuthProvider";

/**
 * Bridges server-side preferences with the next-themes ThemeProvider.
 *
 * - When the user logs in (or on first load), reads /settings/me and pushes
 *   the stored theme into next-themes.
 * - When the user flips the ThemeToggle, persists the change to the server.
 *
 * Also exposes a `setPrefs` helper for the Settings page to update other
 * fields (default chart, language, export delimiter).
 */
export function usePrefs() {
  const { user, updateUser } = useAuth();
  const { theme: activeTheme, setTheme } = useTheme();
  const [prefs, setPrefsState] = useState<AuthUserPreferences | null>(null);
  const [hydrated, setHydrated] = useState(false);

  // Initial load.
  useEffect(() => {
    if (!user) {
      setPrefsState(null);
      setHydrated(false);
      return;
    }
    let cancelled = false;
    api
      .get<AuthUserPreferences>("/settings/me")
      .then((value) => {
        if (cancelled) return;
        setPrefsState(value);
        setTheme(value.theme);
        setHydrated(true);
      })
      .catch(() => {
        if (cancelled) return;
        setHydrated(true);
      });
    return () => {
      cancelled = true;
    };
    // We intentionally only re-run on user identity changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.id]);

  // When the user flips the toggle locally, persist the change.
  useEffect(() => {
    if (!user || !hydrated || !prefs) return;
    if (!activeTheme || activeTheme === prefs.theme) return;
    const next = activeTheme as AuthUserPreferences["theme"];
    api
      .put<AuthUserPreferences>("/settings/me", { json: { theme: next } })
      .then((value) => {
        setPrefsState(value);
        updateUser({ preferences: value });
      })
      .catch(() => {
        /* Non-blocking: theme still applied locally. */
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTheme, hydrated]);

  const setPrefs = useCallback(
    async (patch: Partial<AuthUserPreferences>) => {
      const value = await api.put<AuthUserPreferences>("/settings/me", { json: patch });
      setPrefsState(value);
      updateUser({ preferences: value });
      if (patch.theme) setTheme(patch.theme);
      return value;
    },
    [setTheme, updateUser],
  );

  return { prefs, hydrated, setPrefs };
}
