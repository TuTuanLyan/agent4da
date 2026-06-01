"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { Star } from "lucide-react";
import { api } from "@/lib/api";
import { useAuth } from "@/components/auth/AuthProvider";
import { cn } from "@/lib/utils";

interface Favorite {
  run_id: string;
  question: string;
  status: string;
  is_favorite: boolean;
}

interface HistoryPage {
  items: Favorite[];
  total: number;
}

/** Latest-5 favorites shortlist. Re-fetches when:
 *   - the user signs in,
 *   - any FavoriteToggle fires the global "agent4da:favorites-changed" event.
 */
export function SidebarFavorites() {
  const { user } = useAuth();
  const [items, setItems] = useState<Favorite[]>([]);
  const [loaded, setLoaded] = useState(false);

  const refresh = useCallback(async () => {
    if (!user) {
      setItems([]);
      return;
    }
    try {
      const res = await api.get<HistoryPage>("/history?favorite=true&limit=5&page=1");
      setItems(res.items);
    } catch {
      // Quiet failure: the shortlist is nice-to-have.
    } finally {
      setLoaded(true);
    }
  }, [user]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    function onChange() {
      void refresh();
    }
    window.addEventListener("agent4da:favorites-changed", onChange);
    return () => window.removeEventListener("agent4da:favorites-changed", onChange);
  }, [refresh]);

  return (
    <div className="mt-6 px-3">
      <p className="flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-text-secondary">
        <Star className="h-3 w-3" aria-hidden="true" /> Favorites
      </p>

      {!loaded && (
        <p className="mt-2 text-xs text-text-secondary">Loading...</p>
      )}

      {loaded && items.length === 0 && (
        <p className="mt-2 text-xs text-text-secondary">
          Star a run in History to pin it here.
        </p>
      )}

      {items.length > 0 && (
        <ul className="mt-2 space-y-0.5">
          {items.map((item) => (
            <li key={item.run_id}>
              <Link
                href={`/history/${item.run_id}`}
                title={item.question}
                className={cn(
                  "block truncate rounded-md px-2 py-1 text-xs text-text-secondary",
                  "hover:bg-background hover:text-text-primary",
                )}
              >
                {item.question || "(empty)"}
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
