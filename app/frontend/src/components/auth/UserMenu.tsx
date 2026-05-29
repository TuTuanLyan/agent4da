"use client";

import { useEffect, useRef, useState } from "react";
import { LogOut, ShieldCheck, User as UserIcon } from "lucide-react";
import { useAuth } from "./AuthProvider";
import { cn } from "@/lib/utils";

/** Avatar button + dropdown showing email, role, and Sign out. */
export function UserMenu() {
  const { user, signOut } = useAuth();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function onClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  if (!user) {
    return null;
  }

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        aria-label={`Account: ${user.email}`}
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "inline-flex h-8 w-8 items-center justify-center rounded-full border border-border bg-background",
          "text-text-secondary hover:text-text-primary",
        )}
      >
        <UserIcon className="h-4 w-4" aria-hidden="true" />
      </button>

      {open && (
        <div
          role="menu"
          className="absolute right-0 z-30 mt-2 w-64 rounded-md border border-border bg-elevated p-1 shadow-card"
        >
          <div className="border-b border-border px-3 py-2 text-xs">
            <p className="truncate text-text-primary">{user.email}</p>
            <p className="mt-0.5 flex items-center gap-1 text-text-secondary">
              {user.role === "admin" && (
                <ShieldCheck className="h-3 w-3 text-accent" aria-hidden="true" />
              )}
              <span>Role: {user.role}</span>
            </p>
          </div>

          <button
            type="button"
            onClick={() => {
              setOpen(false);
              void signOut();
            }}
            role="menuitem"
            className={cn(
              "flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-xs",
              "text-text-secondary hover:bg-background hover:text-text-primary",
            )}
          >
            <LogOut className="h-3.5 w-3.5" aria-hidden="true" />
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}
