"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Bot,
  CheckCircle2,
  Cpu,
  Languages,
  Palette,
  PlugZap,
  RotateCcw,
  SlidersHorizontal,
  XCircle,
} from "lucide-react";
import { SettingsSection } from "@/components/settings/SettingsSection";
import { useHealth, type ServiceSnapshot, type ServiceStatus } from "@/hooks/useHealth";
import { usePrefs } from "@/hooks/usePrefs";
import { api, ApiError } from "@/lib/api";
import type { AuthUserPreferences } from "@/lib/auth";
import { cn } from "@/lib/utils";

type SystemConfigured = "configured" | "missing";

interface SystemStatus {
  trino: SystemConfigured;
  airflow: SystemConfigured;
  minio: SystemConfigured;
  gemini: SystemConfigured;
  groq: SystemConfigured;
  llm_provider: string;
  allow_temperature_override: boolean;
  model_whitelist: string[];
  agent_engine: "legacy" | "v2";
}

const THEME_OPTIONS: Array<{
  value: AuthUserPreferences["theme"];
  label: string;
  description: string;
}> = [
  { value: "light", label: "Light", description: "Clean bright workspace." },
  { value: "dark", label: "Dark", description: "Low-glare slate surfaces." },
  { value: "system", label: "System", description: "Follow this device." },
];

const CHART_TYPES: Array<AuthUserPreferences["default_chart_type"]> = [
  "auto",
  "bar",
  "line",
  "pie",
  "table",
];

const LANGUAGES: Array<AuthUserPreferences["preferred_language"]> = ["vi", "en"];

const DELIMITERS = [
  { label: "Comma (,)", value: "," },
  { label: "Semicolon (;)", value: ";" },
  { label: "Tab", value: "\t" },
];

type ModelProvider = "auto" | "gemini" | "groq";

function providerForModel(model: string | null | undefined): ModelProvider {
  if (!model) return "auto";
  const normalized = model.toLowerCase();
  if (normalized.startsWith("gemini-")) return "gemini";
  return "groq";
}

export default function SettingsPage() {
  const { prefs, hydrated, setPrefs } = usePrefs();
  const { data: health, loading: healthLoading, refresh: refreshHealth } = useHealth();
  const [system, setSystem] = useState<SystemStatus | null>(null);
  const [systemLoading, setSystemLoading] = useState(true);
  const [savingKey, setSavingKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [savedMessage, setSavedMessage] = useState<string | null>(null);

  const loadSystem = useCallback(async () => {
    setSystemLoading(true);
    setError(null);
    try {
      const value = await api.get<SystemStatus>("/settings/system");
      setSystem(value);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to load system settings.");
    } finally {
      setSystemLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadSystem();
  }, [loadSystem]);

  const modelOptions = useMemo(() => system?.model_whitelist ?? [], [system]);
  const geminiModels = useMemo(
    () => modelOptions.filter((model) => model.toLowerCase().startsWith("gemini-")),
    [modelOptions],
  );
  const groqModels = useMemo(
    () => modelOptions.filter((model) => !model.toLowerCase().startsWith("gemini-")),
    [modelOptions],
  );
  const effectivePrefs = prefs ?? {
    theme: "system",
    default_chart_type: "auto",
    default_model: null,
    preferred_language: "vi",
    export_delimiter: ",",
  };
  const selectedProvider = providerForModel(effectivePrefs.default_model);
  const filteredModelOptions =
    selectedProvider === "gemini"
      ? geminiModels
      : selectedProvider === "groq"
        ? groqModels
        : modelOptions;

  const updatePrefs = useCallback(
    async (key: string, patch: Partial<AuthUserPreferences>) => {
      setSavingKey(key);
      setError(null);
      setSavedMessage(null);
      try {
        await setPrefs(patch);
        setSavedMessage("Saved.");
        window.setTimeout(() => setSavedMessage(null), 1800);
      } catch (err) {
        setError(err instanceof ApiError ? err.message : "Failed to save preferences.");
      } finally {
        setSavingKey(null);
      }
    },
    [setPrefs],
  );

  return (
    <div className="mx-auto flex max-w-5xl flex-col gap-4">
      <header className="flex flex-col justify-between gap-3 sm:flex-row sm:items-end">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">Settings</h1>
          <p className="text-xs text-text-secondary">
            Personal preferences and redacted system status. Secrets are never shown here.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {savedMessage && (
            <span className="rounded-md border border-success/30 bg-success/10 px-2 py-1 text-xs text-success">
              {savedMessage}
            </span>
          )}
          <button
            type="button"
            onClick={() => {
              void loadSystem();
              void refreshHealth();
            }}
            disabled={systemLoading || healthLoading}
            className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-text-secondary hover:text-text-primary disabled:opacity-50"
          >
            <RotateCcw
              className={cn("h-3.5 w-3.5", (systemLoading || healthLoading) && "animate-spin")}
              aria-hidden="true"
            />
            Refresh status
          </button>
        </div>
      </header>

      {error && (
        <div
          role="alert"
          className="rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error"
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium">Settings error</span>
            <button
              type="button"
              onClick={() => {
                setError(null);
                void loadSystem();
              }}
              className="rounded border border-error/40 px-2 py-0.5 text-xs hover:bg-error/10"
            >
              Retry
            </button>
          </div>
          <p className="mt-1 text-xs">{error}</p>
        </div>
      )}

      <SettingsSection
        title="Theme"
        description="Choose the interface mode. The change is saved to your account and applied immediately."
      >
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          {THEME_OPTIONS.map((option) => {
            const active = effectivePrefs.theme === option.value;
            return (
              <button
                key={option.value}
                type="button"
                onClick={() => void updatePrefs("theme", { theme: option.value })}
                disabled={!hydrated || savingKey === "theme"}
                aria-pressed={active}
                className={cn(
                  "rounded-lg border p-3 text-left transition-colors",
                  "hover:border-accent hover:bg-accent/5 disabled:opacity-60",
                  active
                    ? "border-accent bg-accent/10"
                    : "border-border bg-elevated",
                )}
              >
                <div className="flex items-center gap-2">
                  <Palette className="h-4 w-4 text-accent" aria-hidden="true" />
                  <span className="text-sm font-medium text-text-primary">{option.label}</span>
                </div>
                <p className="mt-1 text-xs text-text-secondary">{option.description}</p>
              </button>
            );
          })}
        </div>
      </SettingsSection>

      <SettingsSection
        title="Model & Agent"
        description="The backend can use Gemini, Groq, or auto fallback. Temperature stays fixed at 0 unless the backend enables overrides."
      >
        <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
          <label className="flex flex-col gap-1 rounded-lg border border-border bg-elevated p-3">
            <span className="flex items-center gap-2 text-xs font-medium text-text-secondary">
              <Bot className="h-4 w-4 text-accent" aria-hidden="true" />
              Provider
            </span>
            <select
              value={selectedProvider}
              disabled={!hydrated || savingKey === "provider"}
              onChange={(event) => {
                const provider = event.target.value as ModelProvider;
                const nextModel =
                  provider === "gemini"
                    ? geminiModels[0] ?? null
                    : provider === "groq"
                      ? groqModels[0] ?? null
                      : null;
                void updatePrefs("provider", { default_model: nextModel });
              }}
              className="rounded-md border border-border bg-surface px-2 py-1.5 text-sm text-text-primary focus:border-accent focus:outline-none"
            >
              <option value="auto">Auto fallback</option>
              <option value="gemini" disabled={geminiModels.length === 0}>
                Gemini
              </option>
              <option value="groq" disabled={groqModels.length === 0}>
                Groq
              </option>
            </select>
          </label>
          <label className="flex flex-col gap-1 rounded-lg border border-border bg-elevated p-3">
            <span className="text-xs font-medium text-text-secondary">Default model</span>
            <select
              value={effectivePrefs.default_model ?? ""}
              disabled={
                !hydrated ||
                selectedProvider === "auto" ||
                filteredModelOptions.length === 0 ||
                savingKey === "model"
              }
              onChange={(event) =>
                void updatePrefs("model", { default_model: event.target.value || null })
              }
              className="rounded-md border border-border bg-surface px-2 py-1.5 text-sm text-text-primary focus:border-accent focus:outline-none"
            >
              {selectedProvider === "auto" ? (
                <option value="">Gemini first, Groq fallback</option>
              ) : filteredModelOptions.length === 0 ? (
                <option value="">No models configured</option>
              ) : (
                filteredModelOptions.map((model) => (
                  <option key={model} value={model}>
                    {model}
                  </option>
                ))
              )}
            </select>
          </label>
          <ReadOnlyField
            icon={SlidersHorizontal}
            label="Temperature"
            value={system?.allow_temperature_override ? "0 (override allowed)" : "0 (locked)"}
          />
          <ReadOnlyField
            icon={Cpu}
            label="Agent engine"
            value={
              systemLoading
                ? "Loading..."
                : system?.agent_engine === "v2"
                  ? "Agent v2"
                  : "Agent (legacy)"
            }
          />
        </div>
        <p className="mt-3 text-xs text-text-secondary">
          The agent engine is set by the backend environment variable{" "}
          <code className="rounded bg-elevated px-1 py-0.5">APP_AGENT_ENGINE</code> and is
          read-only here. Changing it requires recreating the backend container.
        </p>
      </SettingsSection>

      <SettingsSection
        title="Connection Status"
        description="Live health uses /ops/health where available. Configuration status is redacted to configured or missing."
      >
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <ConnectionRow
            label="Trino"
            configured={system?.trino}
            snapshot={health.trino}
          />
          <ConnectionRow
            label="Spark"
            configured="configured"
            snapshot={health.spark}
          />
          <ConnectionRow
            label="Airflow"
            configured={system?.airflow}
            snapshot={health.airflow}
          />
          <ConnectionRow
            label="MinIO"
            configured={system?.minio}
            snapshot={null}
          />
          <ConnectionRow
            label="Gemini"
            configured={system?.gemini}
            snapshot={health.gemini}
          />
          <ConnectionRow
            label="Groq"
            configured={system?.groq}
            snapshot={health.groq}
          />
        </div>
      </SettingsSection>

      <SettingsSection
        title="Defaults"
        description="These preferences control chart selection, CSV export, and default answer language."
      >
        <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
          <label className="flex flex-col gap-1 rounded-lg border border-border bg-elevated p-3">
            <span className="text-xs font-medium text-text-secondary">Default chart</span>
            <select
              value={effectivePrefs.default_chart_type}
              disabled={!hydrated || savingKey === "default_chart_type"}
              onChange={(event) =>
                void updatePrefs("default_chart_type", {
                  default_chart_type: event.target.value as AuthUserPreferences["default_chart_type"],
                })
              }
              className="rounded-md border border-border bg-surface px-2 py-1.5 text-sm text-text-primary focus:border-accent focus:outline-none"
            >
              {CHART_TYPES.map((type) => (
                <option key={type} value={type}>
                  {type}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 rounded-lg border border-border bg-elevated p-3">
            <span className="text-xs font-medium text-text-secondary">CSV delimiter</span>
            <select
              value={effectivePrefs.export_delimiter}
              disabled={!hydrated || savingKey === "export_delimiter"}
              onChange={(event) =>
                void updatePrefs("export_delimiter", {
                  export_delimiter: event.target.value,
                })
              }
              className="rounded-md border border-border bg-surface px-2 py-1.5 text-sm text-text-primary focus:border-accent focus:outline-none"
            >
              {DELIMITERS.map((delimiter) => (
                <option key={delimiter.label} value={delimiter.value}>
                  {delimiter.label}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 rounded-lg border border-border bg-elevated p-3">
            <span className="text-xs font-medium text-text-secondary">Preferred language</span>
            <select
              value={effectivePrefs.preferred_language}
              disabled={!hydrated || savingKey === "preferred_language"}
              onChange={(event) =>
                void updatePrefs("preferred_language", {
                  preferred_language: event.target.value as AuthUserPreferences["preferred_language"],
                })
              }
              className="rounded-md border border-border bg-surface px-2 py-1.5 text-sm text-text-primary focus:border-accent focus:outline-none"
            >
              {LANGUAGES.map((language) => (
                <option key={language} value={language}>
                  {language === "vi" ? "Vietnamese" : "English"}
                </option>
              ))}
            </select>
          </label>
        </div>

        {savingKey && (
          <p className="mt-3 inline-flex items-center gap-1 text-xs text-text-secondary">
            <Languages className="h-3.5 w-3.5 animate-pulse" aria-hidden="true" />
            Saving {savingKey.replaceAll("_", " ")}...
          </p>
        )}
      </SettingsSection>
    </div>
  );
}

function ReadOnlyField({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Bot;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-elevated p-3">
      <div className="flex items-center gap-2 text-xs font-medium text-text-secondary">
        <Icon className="h-4 w-4 text-accent" aria-hidden="true" />
        {label}
      </div>
      <p className="mt-1 truncate text-sm font-semibold text-text-primary">{value}</p>
    </div>
  );
}

function ConnectionRow({
  label,
  configured,
  snapshot,
}: {
  label: string;
  configured?: SystemConfigured;
  snapshot: ServiceSnapshot | null;
}) {
  const liveStatus = snapshot?.status ?? "unknown";
  const configuredStatus = configured ?? "missing";
  const healthy = isHealthy(liveStatus, configuredStatus, snapshot === null);
  const Icon = healthy ? CheckCircle2 : XCircle;
  const detail = snapshot?.detail ?? statusText(liveStatus);

  return (
    <div className="flex items-start justify-between gap-3 rounded-lg border border-border bg-elevated p-3">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <PlugZap className="h-4 w-4 text-accent" aria-hidden="true" />
          <span className="text-sm font-medium text-text-primary">{label}</span>
        </div>
        <p className="mt-1 text-xs text-text-secondary">
          Config: {configuredStatus}
          {snapshot?.latency_ms != null ? ` - ${snapshot.latency_ms} ms` : ""}
        </p>
        {detail && (
          <p className="mt-0.5 truncate text-xs text-text-secondary" title={detail}>
            {detail}
          </p>
        )}
      </div>
      <span
        className={cn(
          "inline-flex shrink-0 items-center gap-1 rounded-md border px-2 py-1 text-xs font-medium",
          healthy
            ? "border-success/30 bg-success/10 text-success"
            : "border-error/30 bg-error/10 text-error",
        )}
      >
        <Icon className="h-3.5 w-3.5" aria-hidden="true" />
        {healthy ? "Ready" : "Check"}
      </span>
    </div>
  );
}

function isHealthy(
  liveStatus: ServiceStatus,
  configured: SystemConfigured,
  noLiveProbe: boolean,
): boolean {
  if (configured === "missing") return false;
  if (noLiveProbe) return true;
  return liveStatus === "ok" || liveStatus === "configured";
}

function statusText(status: ServiceStatus): string {
  switch (status) {
    case "ok":
      return "Live probe OK.";
    case "degraded":
      return "Live probe degraded.";
    case "down":
      return "Live probe down.";
    case "configured":
      return "Configured. No live external call required.";
    case "missing":
      return "Missing required configuration.";
    default:
      return "Live probe not available yet.";
  }
}
