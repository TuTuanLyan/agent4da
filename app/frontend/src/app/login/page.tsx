"use client";

import { FormEvent, useState } from "react";
import { ApiError } from "@/lib/api";
import { useAuth } from "@/components/auth/AuthProvider";
import { cn } from "@/lib/utils";

export default function LoginPage() {
  const { signIn, loading, user } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (loading || user) {
    // Either the cookie-based refresh is resolving, or we're already signed
    // in and the AuthProvider is about to redirect us.
    return (
      <div className="mx-auto flex min-h-[60vh] max-w-md items-center justify-center">
        <p className="text-sm text-text-secondary">Loading...</p>
      </div>
    );
  }

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await signIn(email, password);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.message);
      } else {
        setError("Could not reach the server. Is the backend up?");
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="mx-auto flex min-h-[70vh] max-w-md items-center">
      <form
        onSubmit={onSubmit}
        className="w-full space-y-4 rounded-lg border border-border bg-surface p-6 shadow-card"
        noValidate
      >
        <div>
          <h1 className="text-lg font-semibold text-text-primary">Sign in</h1>
          <p className="mt-1 text-sm text-text-secondary">
            Agent4DA Analytics Console
          </p>
        </div>

        <div className="space-y-1">
          <label htmlFor="email" className="block text-xs font-medium text-text-secondary">
            Email
          </label>
          <input
            id="email"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoComplete="email"
            required
            className={cn(
              "block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-text-primary",
              "placeholder:text-text-secondary focus:border-accent focus:outline-none",
            )}
            placeholder="admin@example.com"
          />
        </div>

        <div className="space-y-1">
          <label htmlFor="password" className="block text-xs font-medium text-text-secondary">
            Password
          </label>
          <input
            id="password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            required
            className={cn(
              "block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-text-primary",
              "placeholder:text-text-secondary focus:border-accent focus:outline-none",
            )}
          />
        </div>

        {error && (
          <p
            role="alert"
            className="rounded-md border border-error/40 bg-error/10 px-3 py-2 text-xs text-error"
          >
            {error}
          </p>
        )}

        <button
          type="submit"
          disabled={submitting}
          className={cn(
            "block w-full rounded-md bg-accent px-3 py-2 text-sm font-medium text-white",
            "hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60",
          )}
        >
          {submitting ? "Signing in..." : "Sign in"}
        </button>

        <p className="text-[11px] text-text-secondary">
          The seeded admin uses APP_BOOTSTRAP_ADMIN_EMAIL / APP_BOOTSTRAP_ADMIN_PASSWORD
          from envs/app.env.
        </p>
      </form>
    </div>
  );
}
