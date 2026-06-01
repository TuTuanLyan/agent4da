/**
 * Backend fetch wrapper.
 *
 * Responsibilities:
 *  - prepend the API base URL,
 *  - attach the access token from in-memory storage,
 *  - on a 401, try POST /auth/refresh (cookie-based) exactly once and retry,
 *  - serialize JSON request/response,
 *  - surface clean ApiError objects to callers.
 *
 * The refresh cookie is HttpOnly and set with path=/auth, so the browser
 * sends it automatically only to /auth/refresh and /auth/logout. The
 * frontend never sees the refresh token.
 */

import { getAccessToken, setAccessToken } from "./auth";

export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ?? "http://localhost:8083";

export interface ApiErrorBody {
  detail?: string | unknown;
}

export class ApiError extends Error {
  status: number;
  body: ApiErrorBody | null;
  constructor(status: number, message: string, body: ApiErrorBody | null) {
    super(message);
    this.status = status;
    this.body = body;
    this.name = "ApiError";
  }
}

export interface ApiOptions extends Omit<RequestInit, "body"> {
  /** Pass a plain object; the wrapper JSON-stringifies it. */
  json?: unknown;
  /** Internal: don't try to refresh on 401 (used by the refresh call itself). */
  skipRefresh?: boolean;
  /** Pass an AbortSignal to allow Stop / cancellation. */
  signal?: AbortSignal;
}

interface RefreshResponse {
  access_token: string;
  token_type: "bearer";
  expires_in: number;
  user: unknown;
}

let refreshInFlight: Promise<RefreshResponse | null> | null = null;
const REQUEST_TIMEOUT_MS = 12_000;

function formatErrorDetail(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (typeof detail === "number" || typeof detail === "boolean") return String(detail);
  if (detail == null) return "";

  if (Array.isArray(detail)) {
    return detail
      .map((item) => formatErrorDetail(item))
      .filter(Boolean)
      .join("; ");
  }

  if (typeof detail === "object") {
    const record = detail as Record<string, unknown>;
    const message = record.detail ?? record.message ?? record.msg;
    if (message !== undefined) return formatErrorDetail(message);

    try {
      return JSON.stringify(detail);
    } catch {
      return "";
    }
  }

  return "";
}

async function fetchWithTimeout(
  input: RequestInfo | URL,
  init: RequestInit = {},
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  const parentSignal = init.signal;

  if (parentSignal?.aborted) {
    controller.abort();
  } else if (parentSignal) {
    parentSignal.addEventListener("abort", () => controller.abort(), { once: true });
  }

  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } finally {
    window.clearTimeout(timeout);
  }
}

async function rawRefresh(): Promise<RefreshResponse | null> {
  try {
    const res = await fetchWithTimeout(`${API_BASE_URL}/auth/refresh`, {
      method: "POST",
      credentials: "include",
    });
    if (!res.ok) return null;
    return (await res.json()) as RefreshResponse;
  } catch {
    return null;
  }
}

async function tryRefresh(): Promise<RefreshResponse | null> {
  if (!refreshInFlight) {
    refreshInFlight = rawRefresh().finally(() => {
      refreshInFlight = null;
    });
  }
  return refreshInFlight;
}

export async function apiFetch<T = unknown>(
  path: string,
  options: ApiOptions = {},
): Promise<T> {
  const { json, skipRefresh, headers, ...rest } = options;
  const url = path.startsWith("http") ? path : `${API_BASE_URL}${path}`;

  const finalHeaders = new Headers(headers ?? {});
  if (json !== undefined) {
    finalHeaders.set("Content-Type", "application/json");
  }
  const token = getAccessToken();
  if (token) finalHeaders.set("Authorization", `Bearer ${token}`);

  const init: RequestInit = {
    ...rest,
    headers: finalHeaders,
    credentials: "include",
    body: json !== undefined ? JSON.stringify(json) : undefined,
  };

  let response = await fetchWithTimeout(url, init);

  if (response.status === 401 && !skipRefresh) {
    const refreshed = await tryRefresh();
    if (refreshed) {
      setAccessToken(refreshed.access_token);
      finalHeaders.set("Authorization", `Bearer ${refreshed.access_token}`);
      response = await fetchWithTimeout(url, { ...init, headers: finalHeaders });
    }
  }

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get("content-type") ?? "";
  const isJson = contentType.includes("application/json");
  const body = isJson ? await response.json().catch(() => null) : await response.text().catch(() => "");

  if (!response.ok) {
    const detail =
      body && typeof body === "object" && "detail" in (body as object)
        ? formatErrorDetail((body as ApiErrorBody).detail)
        : formatErrorDetail(body);
    const message = detail || `Request failed with status ${response.status}`;
    throw new ApiError(response.status, message, isJson ? (body as ApiErrorBody) : null);
  }

  return body as T;
}

export const api = {
  get: <T = unknown>(path: string, opts?: ApiOptions) =>
    apiFetch<T>(path, { ...opts, method: "GET" }),
  post: <T = unknown>(path: string, opts?: ApiOptions) =>
    apiFetch<T>(path, { ...opts, method: "POST" }),
  patch: <T = unknown>(path: string, opts?: ApiOptions) =>
    apiFetch<T>(path, { ...opts, method: "PATCH" }),
  put: <T = unknown>(path: string, opts?: ApiOptions) =>
    apiFetch<T>(path, { ...opts, method: "PUT" }),
  del: <T = unknown>(path: string, opts?: ApiOptions) =>
    apiFetch<T>(path, { ...opts, method: "DELETE" }),
};
