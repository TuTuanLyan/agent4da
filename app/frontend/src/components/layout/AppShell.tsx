"use client";

import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import { Sidebar } from "./Sidebar";
import { Topbar } from "./Topbar";
import { useAuth } from "@/components/auth/AuthProvider";
import { usePrefs } from "@/hooks/usePrefs";

const CHROMELESS_ROUTES = new Set(["/login"]);

/** Renders the sidebar + topbar shell on private routes, and a plain
 *  centered container on auth / public routes. */
export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const { loading, user } = useAuth();
  // Side-effect only: synchronizes server-stored prefs with next-themes.
  usePrefs();

  const chromeless = CHROMELESS_ROUTES.has(pathname);

  if (chromeless) {
    return <main className="min-h-screen bg-background p-4 md:p-8">{children}</main>;
  }

  // Wait until auth has resolved AND the user is known before painting the
  // dashboard chrome. The AuthProvider's effect will redirect to /login on
  // the next tick if `user` is still null.
  if (loading || !user) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-background">
        <p className="text-sm text-text-secondary">Loading...</p>
      </main>
    );
  }

  return (
    <div className="flex min-h-screen flex-row">
      <Sidebar />
      <div className="flex min-h-screen flex-1 flex-col">
        <Topbar />
        <main className="flex-1 bg-background p-4 md:p-6">{children}</main>
      </div>
    </div>
  );
}
