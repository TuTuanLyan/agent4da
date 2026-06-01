"use client";

import { useState } from "react";
import { Star } from "lucide-react";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

interface Props {
  runId: string;
  initial: boolean;
  onChange?: (next: boolean) => void;
}

export function FavoriteToggle({ runId, initial, onChange }: Props) {
  const [favorite, setFavorite] = useState(initial);
  const [pending, setPending] = useState(false);

  async function toggle(e: React.MouseEvent) {
    e.stopPropagation();
    e.preventDefault();
    if (pending) return;
    const next = !favorite;
    setFavorite(next);
    setPending(true);
    try {
      if (next) await api.post(`/history/${runId}/favorite`);
      else await api.del(`/history/${runId}/favorite`);
      onChange?.(next);
      // Let the sidebar refresh its shortlist.
      if (typeof window !== "undefined") {
        window.dispatchEvent(new CustomEvent("agent4da:favorites-changed"));
      }
    } catch {
      // Revert on failure.
      setFavorite(!next);
    } finally {
      setPending(false);
    }
  }

  return (
    <button
      type="button"
      onClick={toggle}
      aria-label={favorite ? "Unstar" : "Star"}
      aria-pressed={favorite}
      className={cn(
        "rounded p-1 transition-colors",
        favorite ? "text-warning" : "text-text-secondary hover:text-text-primary",
        pending && "opacity-50",
      )}
    >
      <Star
        className={cn("h-3.5 w-3.5", favorite && "fill-current")}
        aria-hidden="true"
      />
    </button>
  );
}
