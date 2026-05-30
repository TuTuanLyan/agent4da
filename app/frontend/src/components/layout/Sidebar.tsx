"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { NAV_ITEMS } from "@/lib/nav";
import { cn } from "@/lib/utils";
import { SidebarFavorites } from "./SidebarFavorites";

/** Left sidebar with the five primary destinations.
 *
 *  Active item carries a left-edge accent bar plus an accent text color.
 *  In Phase 1 the Favorites shortlist is a placeholder block; it's populated
 *  in Phase 4.
 */
export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside
      aria-label="Primary"
      className="hidden w-56 shrink-0 flex-col border-r border-border bg-surface md:flex"
    >
      <div className="flex h-14 items-center px-4">
        <Link href="/ask" className="flex items-center gap-2">
          <span
            aria-hidden="true"
            className="inline-block h-6 w-6 rounded-md bg-accent/15 ring-1 ring-accent/30"
          />
          <span className="text-sm font-semibold text-text-primary">Agent4DA</span>
        </Link>
      </div>

      <nav className="flex-1 px-2 py-2">
        <ul className="space-y-0.5">
          {NAV_ITEMS.map(({ href, label, icon: Icon }) => {
            const active = pathname === href || pathname.startsWith(`${href}/`);
            return (
              <li key={href} className="relative">
                {active && (
                  <span
                    aria-hidden="true"
                    className="absolute inset-y-1 left-0 w-0.5 rounded-r bg-accent"
                  />
                )}
                <Link
                  href={href}
                  aria-current={active ? "page" : undefined}
                  className={cn(
                    "flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors",
                    active
                      ? "bg-accent/10 text-accent"
                      : "text-text-secondary hover:bg-background hover:text-text-primary",
                  )}
                >
                  <Icon className="h-4 w-4" aria-hidden="true" />
                  <span>{label}</span>
                </Link>
              </li>
            );
          })}
        </ul>

        <SidebarFavorites />
      </nav>

      <div className="px-4 py-3 text-xs text-text-secondary">v0.1</div>
    </aside>
  );
}
