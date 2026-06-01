"use client";

import { FormEvent, useState } from "react";
import { ApiError } from "@/lib/api";
import { useAuth } from "@/components/auth/AuthProvider";
import { cn } from "@/lib/utils";

export default function LoginPage() {
  const { signIn, signUp, loading, user } = useAuth();
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
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
    if (mode === "signup") {
      if (password.length < 8) {
        setError("Password must be at least 8 characters.");
        return;
      }
      if (password !== confirmPassword) {
        setError("Passwords do not match.");
        return;
      }
    }
    setSubmitting(true);
    try {
      if (mode === "signup") {
        await signUp(email, password);
      } else {
        await signIn(email, password);
      }
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
          <h1 className="text-lg font-semibold text-text-primary">
            {mode === "signin" ? "Sign in" : "Create account"}
          </h1>
          <p className="mt-1 text-sm text-text-secondary">
            Agent4DA Analytics Console
          </p>
        </div>

        <div className="grid grid-cols-2 rounded-md border border-border bg-background p-1">
          <button
            type="button"
            onClick={() => {
              setMode("signin");
              setError(null);
            }}
            className={cn(
              "rounded px-3 py-1.5 text-xs font-medium",
              mode === "signin"
                ? "bg-surface text-text-primary shadow-sm"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            Sign in
          </button>
          <button
            type="button"
            onClick={() => {
              setMode("signup");
              setError(null);
            }}
            className={cn(
              "rounded px-3 py-1.5 text-xs font-medium",
              mode === "signup"
                ? "bg-surface text-text-primary shadow-sm"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            Create account
          </button>
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
            autoComplete={mode === "signup" ? "new-password" : "current-password"}
            required
            minLength={mode === "signup" ? 8 : undefined}
            className={cn(
              "block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-text-primary",
              "placeholder:text-text-secondary focus:border-accent focus:outline-none",
            )}
          />
        </div>

        {mode === "signup" && (
          <div className="space-y-1">
            <label htmlFor="confirm-password" className="block text-xs font-medium text-text-secondary">
              Confirm password
            </label>
            <input
              id="confirm-password"
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              autoComplete="new-password"
              required
              minLength={8}
              className={cn(
                "block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-text-primary",
                "placeholder:text-text-secondary focus:border-accent focus:outline-none",
              )}
            />
          </div>
        )}

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
          {submitting
            ? mode === "signup"
              ? "Creating account..."
              : "Signing in..."
            : mode === "signup"
              ? "Create account"
              : "Sign in"}
        </button>

        {mode === "signin" ? (
          <p className="text-[11px] text-text-secondary">
            The seeded admin uses APP_BOOTSTRAP_ADMIN_EMAIL / APP_BOOTSTRAP_ADMIN_PASSWORD
            from envs/app.env.
          </p>
        ) : (
          <p className="text-[11px] text-text-secondary">
            New accounts are created with the user role.
          </p>
        )}
      </form>
    </div>
  );
}
