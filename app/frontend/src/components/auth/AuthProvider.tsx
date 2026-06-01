"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { usePathname, useRouter } from "next/navigation";
import { api, ApiError } from "@/lib/api";
import { setAccessToken, type AuthUser } from "@/lib/auth";

interface AuthState {
  user: AuthUser | null;
  loading: boolean;
  signIn: (email: string, password: string) => Promise<void>;
  signUp: (email: string, password: string) => Promise<void>;
  signOut: () => Promise<void>;
  refresh: () => Promise<void>;
  updateUser: (patch: Partial<AuthUser>) => void;
}

const Ctx = createContext<AuthState | null>(null);

const PUBLIC_ROUTES = new Set(["/login"]);

interface TokenResponse {
  access_token: string;
  token_type: "bearer";
  expires_in: number;
  user: AuthUser;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();
  const pathname = usePathname();

  // On boot, try to mint an access token from the refresh cookie.
  const refresh = useCallback(async () => {
    try {
      const res = await api.post<TokenResponse>("/auth/refresh", { skipRefresh: true });
      setAccessToken(res.access_token);
      setUser(res.user);
    } catch {
      setAccessToken(null);
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const signIn = useCallback(
    async (email: string, password: string) => {
      const res = await api.post<TokenResponse>("/auth/login", {
        json: { email, password },
        skipRefresh: true,
      });
      setAccessToken(res.access_token);
      setUser(res.user);
      router.replace("/ask");
    },
    [router],
  );

  const signUp = useCallback(
    async (email: string, password: string) => {
      const res = await api.post<TokenResponse>("/auth/register", {
        json: { email, password },
        skipRefresh: true,
      });
      setAccessToken(res.access_token);
      setUser(res.user);
      router.replace("/ask");
    },
    [router],
  );

  const signOut = useCallback(async () => {
    try {
      await api.post("/auth/logout", { skipRefresh: true });
    } catch {
      // Swallow errors: we still want to clear local state.
    }
    setAccessToken(null);
    setUser(null);
    router.replace("/login");
  }, [router]);

  const updateUser = useCallback((patch: Partial<AuthUser>) => {
    setUser((prev) => (prev ? { ...prev, ...patch } : prev));
  }, []);

  // Route guard: bounce to /login if unauthenticated on a private route.
  useEffect(() => {
    if (loading) return;
    if (!user && !PUBLIC_ROUTES.has(pathname)) {
      router.replace(`/login?next=${encodeURIComponent(pathname)}`);
    }
    if (user && pathname === "/login") {
      router.replace("/ask");
    }
  }, [loading, user, pathname, router]);

  const value = useMemo<AuthState>(
    () => ({ user, loading, signIn, signUp, signOut, refresh, updateUser }),
    [user, loading, signIn, signUp, signOut, refresh, updateUser],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAuth(): AuthState {
  const ctx = useContext(Ctx);
  if (!ctx) {
    throw new Error("useAuth must be used inside <AuthProvider>");
  }
  return ctx;
}

// Re-export to keep the toast/error type discoverable from one place.
export { ApiError };
