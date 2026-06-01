/**
 * In-memory access token storage.
 *
 * The token never lands in localStorage: that would expose it to any XSS.
 * The refresh token lives in an HttpOnly cookie (set by the backend on
 * /auth/login and /auth/refresh).
 *
 * On a hard reload, the module starts with no token; the AuthProvider
 * calls POST /auth/refresh once to mint a fresh access token from the
 * existing cookie.
 */

let accessToken: string | null = null;
const listeners = new Set<(token: string | null) => void>();

export function getAccessToken(): string | null {
  return accessToken;
}

export function setAccessToken(token: string | null): void {
  accessToken = token;
  for (const fn of listeners) fn(token);
}

export function onAccessTokenChange(fn: (token: string | null) => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export interface AuthUser {
  id: string;
  email: string;
  role: "user" | "admin";
  created_at: string;
  preferences: AuthUserPreferences | null;
}

export interface AuthUserPreferences {
  theme: "light" | "dark" | "system";
  default_chart_type: "auto" | "bar" | "line" | "pie" | "table";
  default_model: string | null;
  preferred_language: "vi" | "en";
  export_delimiter: string;
}
