"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  AlertTriangle,
  Check,
  Pencil,
  Pin,
  PinOff,
  Loader2,
  MessageSquareText,
  PanelLeft,
  Plus,
  Trash2,
  X,
} from "lucide-react";
import { AgentStepper } from "@/components/ask/AgentStepper";
import { ChatResultBlock } from "@/components/ask/ChatResultBlock";
import { QuestionInput } from "@/components/ask/QuestionInput";
import { SampleChips } from "@/components/ask/SampleChips";
import { useAgentStream, type StepMap } from "@/hooks/useAgentStream";
import { api, ApiError } from "@/lib/api";
import { formatRelative } from "@/lib/format";
import type { AskResult, ChatSession, ChatSessionSummary } from "@/lib/types";
import { cn } from "@/lib/utils";

const STORAGE_KEY = "agent4da.activeSessionId";

interface ChatTurn {
  id: string;
  question: string;
  result: AskResult | null;
  pending?: boolean;
  error?: string | null;
}

function sessionTitle(session: ChatSessionSummary | ChatSession | null): string {
  return session?.title || "Cuộc trò chuyện mới";
}

function sortSessions(rows: ChatSessionSummary[]): ChatSessionSummary[] {
  return [...rows].sort((a, b) => {
    if (a.is_pinned !== b.is_pinned) return a.is_pinned ? -1 : 1;
    const pinnedDiff =
      new Date(b.pinned_at ?? 0).getTime() - new Date(a.pinned_at ?? 0).getTime();
    if (pinnedDiff !== 0) return pinnedDiff;
    return new Date(b.last_used_at).getTime() - new Date(a.last_used_at).getTime();
  });
}

/** Business-friendly, aria-live status text for the current processing step. */
function statusText(steps: StepMap): string {
  if (steps.load_metadata === "running") return "Đang đọc lược đồ dữ liệu...";
  if (steps.build_prompt === "running") return "Đang hiểu yêu cầu...";
  if (steps.generate_sql === "running") return "Đang tạo câu truy vấn...";
  if (steps.guard_sql === "running") return "Đang kiểm tra truy vấn...";
  if (steps.execute_sql === "running") return "Đang truy vấn dữ liệu...";
  if (steps.summarize === "running") return "Đang phân tích kết quả...";
  return "Đang phân tích yêu cầu...";
}

export default function AskPage() {
  const router = useRouter();
  const params = useSearchParams();
  const { steps, result, error, streaming, start, stop, reset } = useAgentStream();

  const [draft, setDraft] = useState("");
  const [sessions, setSessions] = useState<ChatSessionSummary[]>([]);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [loadingSessions, setLoadingSessions] = useState(true);
  const [loadingTurns, setLoadingTurns] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [confirmingDeleteId, setConfirmingDeleteId] = useState<string | null>(null);
  const [editingSessionId, setEditingSessionId] = useState<string | null>(null);
  const [renameDraft, setRenameDraft] = useState("");
  const [savingSessionId, setSavingSessionId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const pendingIdRef = useRef<string | null>(null);
  const pendingSessionRef = useRef<string | null>(null);
  const timelineEndRef = useRef<HTMLDivElement | null>(null);

  const selectedSession = useMemo(
    () => sessions.find((session) => session.id === selectedSessionId) ?? null,
    [sessions, selectedSessionId],
  );

  const refreshSessions = useCallback(async () => {
    const value = await api.get<ChatSessionSummary[]>("/agent/sessions");
    setSessions(sortSessions(value));
    return value;
  }, []);

  const createSession = useCallback(async () => {
    const created = await api.post<ChatSession>("/agent/sessions");
    const summary: ChatSessionSummary = {
      ...created,
      run_count: 0,
      last_question: null,
      last_status: null,
    };
    setSessions((current) => sortSessions([summary, ...current]));
    setSelectedSessionId(created.id);
    setTurns([]);
    return created.id;
  }, []);

  // Prefill the composer when navigated from "Re-run this question" in History.
  useEffect(() => {
    const q = params.get("question");
    if (q && !draft) setDraft(q);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [params]);

  // Boot: load sessions, pick the active one from URL / storage / first.
  useEffect(() => {
    let cancelled = false;

    async function boot() {
      setLoadingSessions(true);
      setPageError(null);
      try {
        let loaded = await api.get<ChatSessionSummary[]>("/agent/sessions");
        if (loaded.length === 0) {
          const created = await api.post<ChatSession>("/agent/sessions");
          loaded = [{ ...created, run_count: 0, last_question: null, last_status: null }];
        }
        if (cancelled) return;
        loaded = sortSessions(loaded);
        setSessions(loaded);
        const urlSession = params.get("session");
        const storedSession =
          typeof window !== "undefined" ? window.localStorage.getItem(STORAGE_KEY) : null;
        const next =
          loaded.find((session) => session.id === urlSession)?.id ??
          loaded.find((session) => session.id === storedSession)?.id ??
          loaded[0]?.id ??
          null;
        setSelectedSessionId(next);
      } catch (err) {
        if (!cancelled) {
          setPageError(err instanceof ApiError ? err.message : "Không tải được danh sách trò chuyện.");
        }
      } finally {
        if (!cancelled) setLoadingSessions(false);
      }
    }

    void boot();
    return () => {
      cancelled = true;
    };
    // Initial boot only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // When the active session changes: persist it and load its turns.
  useEffect(() => {
    if (!selectedSessionId) return;
    let cancelled = false;

    if (typeof window !== "undefined") {
      window.localStorage.setItem(STORAGE_KEY, selectedSessionId);
      const search = new URLSearchParams(window.location.search);
      search.set("session", selectedSessionId);
      router.replace(`/ask?${search.toString()}`, { scroll: false });
    }

    async function loadTurns() {
      setLoadingTurns(true);
      setPageError(null);
      try {
        const runs = await api.get<AskResult[]>(`/agent/sessions/${selectedSessionId}/runs`);
        if (cancelled) return;
        setTurns(runs.map((run) => ({ id: run.run_id, question: run.question, result: run })));
        reset();
      } catch (err) {
        if (!cancelled) {
          setPageError(err instanceof ApiError ? err.message : "Không tải được cuộc trò chuyện này.");
        }
      } finally {
        if (!cancelled) setLoadingTurns(false);
      }
    }

    void loadTurns();
    return () => {
      cancelled = true;
    };
  }, [reset, router, selectedSessionId]);

  // Fold a finished stream result into the matching pending turn.
  useEffect(() => {
    if (!result) return;
    const pendingId = pendingIdRef.current;
    const targetSession = result.session_id ?? pendingSessionRef.current;

    if (targetSession === selectedSessionId) {
      setTurns((current) => {
        if (pendingId && current.some((turn) => turn.id === pendingId)) {
          return current.map((turn) =>
            turn.id === pendingId
              ? { id: result.run_id, question: result.question, result, pending: false }
              : turn,
          );
        }
        if (current.some((turn) => turn.id === result.run_id)) return current;
        return [...current, { id: result.run_id, question: result.question, result }];
      });
    }

    pendingIdRef.current = null;
    pendingSessionRef.current = null;
    void refreshSessions();
  }, [refreshSessions, result, selectedSessionId]);

  // Surface a stream error on the pending turn.
  useEffect(() => {
    if (!error || streaming || !pendingIdRef.current) return;
    const pendingId = pendingIdRef.current;
    setTurns((current) =>
      current.map((turn) => (turn.id === pendingId ? { ...turn, pending: false, error } : turn)),
    );
    pendingIdRef.current = null;
    pendingSessionRef.current = null;
  }, [error, streaming]);

  // Keep the newest turn in view.
  useEffect(() => {
    timelineEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [turns, streaming]);

  const ensureSession = useCallback(async () => {
    if (selectedSessionId) return selectedSessionId;
    return createSession();
  }, [createSession, selectedSessionId]);

  const runQuestion = useCallback(
    async (question?: string) => {
      const q = (question ?? draft).trim();
      if (!q || streaming) return;

      const sessionId = await ensureSession();
      if (!sessionId) return;

      const pendingId = `pending-${Date.now()}`;
      pendingIdRef.current = pendingId;
      pendingSessionRef.current = sessionId;
      setTurns((current) => [...current, { id: pendingId, question: q, result: null, pending: true }]);
      setDraft("");
      void start(q, { sessionId });
    },
    [draft, ensureSession, start, streaming],
  );

  const stopCurrentRun = useCallback(async () => {
    await stop();
    if (!pendingIdRef.current) return;
    const pendingId = pendingIdRef.current;
    setTurns((current) =>
      current.map((turn) =>
        turn.id === pendingId ? { ...turn, pending: false, error: "Đã dừng theo yêu cầu." } : turn,
      ),
    );
    pendingIdRef.current = null;
    pendingSessionRef.current = null;
  }, [stop]);

  const newChat = useCallback(async () => {
    try {
      await createSession();
      setSidebarOpen(false);
    } catch (err) {
      setPageError(err instanceof ApiError ? err.message : "Không tạo được cuộc trò chuyện mới.");
    }
  }, [createSession]);

  const patchSession = useCallback(
    async (id: string, patch: { title?: string | null; is_pinned?: boolean }) => {
      const previous = sessions;
      const now = new Date().toISOString();
      setSavingSessionId(id);
      setPageError(null);
      setSessions((current) =>
        sortSessions(
          current.map((session) =>
            session.id === id
              ? {
                  ...session,
                  title: Object.prototype.hasOwnProperty.call(patch, "title")
                    ? patch.title ?? null
                    : session.title,
                  is_pinned: patch.is_pinned ?? session.is_pinned,
                  pinned_at:
                    patch.is_pinned === true
                      ? now
                      : patch.is_pinned === false
                        ? null
                        : session.pinned_at,
                }
              : session,
          ),
        ),
      );
      try {
        const updated = await api.patch<ChatSession>(`/agent/sessions/${id}`, { json: patch });
        setSessions((current) =>
          sortSessions(
            current.map((session) =>
              session.id === id
                ? {
                    ...session,
                    title: updated.title,
                    is_pinned: updated.is_pinned,
                    pinned_at: updated.pinned_at,
                    created_at: updated.created_at,
                    last_used_at: updated.last_used_at,
                  }
                : session,
            ),
          ),
        );
      } catch (err) {
        setSessions(previous);
        setPageError(err instanceof ApiError ? err.message : "Không cập nhật được cuộc trò chuyện.");
      } finally {
        setSavingSessionId(null);
      }
    },
    [sessions],
  );

  const beginRename = useCallback((session: ChatSessionSummary) => {
    setConfirmingDeleteId(null);
    setEditingSessionId(session.id);
    setRenameDraft(session.title ?? "");
  }, []);

  const submitRename = useCallback(
    async (id: string) => {
      const title = renameDraft.trim();
      setEditingSessionId(null);
      await patchSession(id, { title: title || null });
    },
    [patchSession, renameDraft],
  );

  const togglePin = useCallback(
    async (session: ChatSessionSummary) => {
      setConfirmingDeleteId(null);
      await patchSession(session.id, { is_pinned: !session.is_pinned });
    },
    [patchSession],
  );

  const deleteSession = useCallback(
    async (id: string) => {
      setConfirmingDeleteId(null);
      try {
        await api.del(`/agent/sessions/${id}`);
        const remaining = sessions.filter((session) => session.id !== id);
        setSessions(remaining);
        if (id === selectedSessionId) {
          // Runs stay in History; pick another thread or open a fresh one.
          if (remaining.length > 0) {
            setSelectedSessionId(remaining[0].id);
          } else {
            await createSession();
          }
        }
      } catch (err) {
        setPageError(err instanceof ApiError ? err.message : "Không xóa được cuộc trò chuyện.");
      }
    },
    [createSession, selectedSessionId, sessions],
  );

  const sessionList = (
    <>
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <MessageSquareText className="h-4 w-4 text-accent" aria-hidden="true" />
          <h1 className="text-sm font-semibold text-text-primary">Trò chuyện</h1>
        </div>
        <button
          type="button"
          onClick={() => void newChat()}
          className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-2 py-1 text-xs text-text-secondary hover:text-text-primary"
          title="Cuộc trò chuyện mới"
        >
          <Plus className="h-4 w-4" aria-hidden="true" />
          Mới
        </button>
      </div>

      {loadingSessions ? (
        <div className="flex items-center gap-2 text-sm text-text-secondary">
          <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
          Đang tải...
        </div>
      ) : (
        <div className="-mr-1 flex-1 space-y-1 overflow-auto pr-1">
          {sessions.map((session) => {
            const active = session.id === selectedSessionId;
            const confirming = confirmingDeleteId === session.id;
            const editing = editingSessionId === session.id;
            const sidebarEditing = editing && !active;
            const saving = savingSessionId === session.id;
            return (
              <div
                key={session.id}
                className={cn(
                  "group flex items-stretch gap-1 rounded-md border transition-colors",
                  active
                    ? "border-accent bg-accent/10"
                    : "border-transparent hover:border-border hover:bg-background",
                  )}
              >
                {sidebarEditing ? (
                  <div className="min-w-0 flex-1 px-2 py-2">
                    <input
                      value={renameDraft}
                      onChange={(event) => setRenameDraft(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") void submitRename(session.id);
                        if (event.key === "Escape") setEditingSessionId(null);
                      }}
                      maxLength={200}
                      autoFocus
                      className="w-full rounded-md border border-border bg-background px-2 py-1 text-sm text-text-primary outline-none focus:border-accent"
                      placeholder="Tên cuộc trò chuyện"
                      aria-label="Tên cuộc trò chuyện"
                    />
                  </div>
                ) : (
                  <button
                    type="button"
                    onClick={() => {
                      setSelectedSessionId(session.id);
                      setSidebarOpen(false);
                    }}
                    aria-pressed={active}
                    className="min-w-0 flex-1 rounded-md px-3 py-2 text-left"
                  >
                    <span className="flex min-w-0 items-center gap-1.5">
                      {session.is_pinned && (
                        <Pin className="h-3 w-3 shrink-0 fill-current text-warning" aria-hidden="true" />
                      )}
                      <span className="block truncate text-sm font-medium text-text-primary">
                        {sessionTitle(session)}
                      </span>
                    </span>
                    <span className="mt-0.5 block truncate text-[11px] text-text-secondary">
                      {session.last_question || "Chưa có câu hỏi"}
                    </span>
                    <span className="mt-0.5 block text-[11px] text-text-secondary">
                      {session.run_count} lượt · {formatRelative(session.last_used_at)}
                    </span>
                  </button>
                )}

                {confirming ? (
                  <div className="flex flex-col items-center justify-center gap-1 pr-1">
                    <button
                      type="button"
                      onClick={() => void deleteSession(session.id)}
                      className="rounded p-1 text-error hover:bg-error/10"
                      title="Xác nhận xóa"
                      aria-label="Xác nhận xóa cuộc trò chuyện"
                    >
                      <Check className="h-3.5 w-3.5" aria-hidden="true" />
                    </button>
                    <button
                      type="button"
                      onClick={() => setConfirmingDeleteId(null)}
                      className="rounded p-1 text-text-secondary hover:bg-background"
                      title="Hủy"
                      aria-label="Hủy xóa"
                    >
                      <X className="h-3.5 w-3.5" aria-hidden="true" />
                    </button>
                  </div>
                ) : (
                  <div className="flex items-center pr-1 text-text-secondary opacity-0 transition-opacity focus-within:opacity-100 group-hover:opacity-100">
                    {sidebarEditing ? (
                      <>
                        <button
                          type="button"
                          onClick={() => void submitRename(session.id)}
                          disabled={saving}
                          className="rounded p-1 hover:bg-background hover:text-success disabled:opacity-50"
                          title="Lưu tên"
                          aria-label="Lưu tên cuộc trò chuyện"
                        >
                          <Check className="h-3.5 w-3.5" aria-hidden="true" />
                        </button>
                        <button
                          type="button"
                          onClick={() => setEditingSessionId(null)}
                          className="rounded p-1 hover:bg-background hover:text-text-primary"
                          title="Hủy đổi tên"
                          aria-label="Hủy đổi tên"
                        >
                          <X className="h-3.5 w-3.5" aria-hidden="true" />
                        </button>
                      </>
                    ) : (
                      <>
                        <button
                          type="button"
                          onClick={() => void togglePin(session)}
                          disabled={saving}
                          className="rounded p-1 hover:bg-background hover:text-warning disabled:opacity-50"
                          title={session.is_pinned ? "Bỏ ghim" : "Ghim cuộc trò chuyện"}
                          aria-label={session.is_pinned ? "Bỏ ghim cuộc trò chuyện" : "Ghim cuộc trò chuyện"}
                        >
                          {session.is_pinned ? (
                            <PinOff className="h-3.5 w-3.5" aria-hidden="true" />
                          ) : (
                            <Pin className="h-3.5 w-3.5" aria-hidden="true" />
                          )}
                        </button>
                        <button
                          type="button"
                          onClick={() => beginRename(session)}
                          className="rounded p-1 hover:bg-background hover:text-text-primary"
                          title="Đổi tên"
                          aria-label={`Đổi tên ${sessionTitle(session)}`}
                        >
                          <Pencil className="h-3.5 w-3.5" aria-hidden="true" />
                        </button>
                        <button
                          type="button"
                          onClick={() => setConfirmingDeleteId(session.id)}
                          className="rounded p-1 hover:bg-error/10 hover:text-error"
                          title="Xóa cuộc trò chuyện"
                          aria-label={`Xóa ${sessionTitle(session)}`}
                        >
                          <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
                        </button>
                      </>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </>
  );

  return (
    <div className="relative flex h-[calc(100dvh-7rem)] gap-4">
      {/* Mobile backdrop when the drawer is open. */}
      {sidebarOpen && (
        <button
          type="button"
          aria-label="Đóng danh sách trò chuyện"
          onClick={() => setSidebarOpen(false)}
          className="absolute inset-0 z-10 bg-black/30 md:hidden"
        />
      )}

      <aside
        className={cn(
          "z-20 flex w-72 shrink-0 flex-col rounded-lg border border-border bg-surface p-3 shadow-card",
          "absolute inset-y-0 left-0 md:static md:flex",
          sidebarOpen ? "flex" : "hidden md:flex",
        )}
      >
        {sessionList}
      </aside>

      <main className="flex min-w-0 flex-1 flex-col rounded-lg border border-border bg-surface shadow-card">
        <header className="flex items-center justify-between gap-2 border-b border-border px-4 py-3">
          <div className="flex min-w-0 items-center gap-2">
            <button
              type="button"
              onClick={() => setSidebarOpen((open) => !open)}
              className="rounded-md border border-border p-1.5 text-text-secondary hover:text-text-primary md:hidden"
              aria-label="Mở danh sách trò chuyện"
            >
              <PanelLeft className="h-4 w-4" aria-hidden="true" />
            </button>
            <div className="min-w-0">
              <p className="text-[11px] uppercase tracking-wide text-text-secondary">Cuộc trò chuyện</p>
              {selectedSession && editingSessionId === selectedSession.id ? (
                <input
                  value={renameDraft}
                  onChange={(event) => setRenameDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") void submitRename(selectedSession.id);
                    if (event.key === "Escape") setEditingSessionId(null);
                  }}
                  maxLength={200}
                  autoFocus
                  className="mt-0.5 w-full rounded-md border border-border bg-background px-2 py-1 text-sm font-semibold text-text-primary outline-none focus:border-accent"
                  placeholder="Tên cuộc trò chuyện"
                  aria-label="Tên cuộc trò chuyện"
                />
              ) : (
                <h2 className="truncate text-sm font-semibold text-text-primary">
                  {sessionTitle(selectedSession)}
                </h2>
              )}
            </div>
          </div>
          {selectedSession && (
            <div className="flex shrink-0 items-center gap-1">
              <button
                type="button"
                onClick={() => void togglePin(selectedSession)}
                disabled={savingSessionId === selectedSession.id}
                className={cn(
                  "rounded-md border border-border p-1.5 text-text-secondary hover:text-warning disabled:opacity-50",
                  selectedSession.is_pinned && "border-warning/40 bg-warning/10 text-warning",
                )}
                title={selectedSession.is_pinned ? "Bỏ ghim" : "Ghim cuộc trò chuyện"}
                aria-label={selectedSession.is_pinned ? "Bỏ ghim cuộc trò chuyện" : "Ghim cuộc trò chuyện"}
              >
                {selectedSession.is_pinned ? (
                  <PinOff className="h-4 w-4" aria-hidden="true" />
                ) : (
                  <Pin className="h-4 w-4" aria-hidden="true" />
                )}
              </button>
              {editingSessionId === selectedSession.id ? (
                <button
                  type="button"
                  onClick={() => void submitRename(selectedSession.id)}
                  disabled={savingSessionId === selectedSession.id}
                  className="rounded-md border border-border p-1.5 text-text-secondary hover:text-success disabled:opacity-50"
                  title="Lưu tên"
                  aria-label="Lưu tên cuộc trò chuyện"
                >
                  <Check className="h-4 w-4" aria-hidden="true" />
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => beginRename(selectedSession)}
                  className="rounded-md border border-border p-1.5 text-text-secondary hover:text-text-primary"
                  title="Đổi tên"
                  aria-label="Đổi tên cuộc trò chuyện"
                >
                  <Pencil className="h-4 w-4" aria-hidden="true" />
                </button>
              )}
              <span className="hidden rounded-md border border-border px-2 py-1 text-[11px] text-text-secondary sm:inline">
                Ngữ cảnh bật · {selectedSession.run_count} lượt
              </span>
            </div>
          )}
        </header>

        {pageError && (
          <div role="alert" className="mx-4 mt-3 rounded-md border border-error/40 bg-error/10 p-3 text-sm text-error">
            <div className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4" aria-hidden="true" />
              {pageError}
            </div>
          </div>
        )}

        <div aria-live="polite" className="flex-1 space-y-4 overflow-auto p-4">
          {loadingTurns ? (
            <div className="flex items-center gap-2 text-sm text-text-secondary">
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
              Đang tải cuộc trò chuyện...
            </div>
          ) : turns.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center gap-4 text-center">
              <p className="max-w-md text-sm text-text-secondary">
                Đặt câu hỏi kinh doanh về doanh thu, khách hàng, sản phẩm hoặc xu hướng. Bạn có thể
                hỏi bằng tiếng Việt hoặc tiếng Anh.
              </p>
              <SampleChips onPick={setDraft} />
            </div>
          ) : (
            <>
              {turns.map((turn) => (
                <article key={turn.id} className="space-y-3">
                  <div className="ml-auto max-w-2xl rounded-2xl rounded-br-sm bg-accent px-4 py-2 text-sm text-white">
                    {turn.question}
                  </div>

                  <div className="max-w-3xl rounded-2xl rounded-bl-sm border border-border bg-background p-3">
                    {turn.pending ? (
                      <div className="space-y-3">
                        <div className="flex items-center gap-3">
                          <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-accent" aria-hidden="true" />
                          <p className="text-sm text-text-secondary">{statusText(steps)}</p>
                        </div>
                        <AgentStepper steps={steps} />
                      </div>
                    ) : turn.error ? (
                      <div role="alert" className="text-sm text-error">
                        {turn.error}
                      </div>
                    ) : turn.result ? (
                      <ChatResultBlock
                        result={turn.result}
                        onRetry={() => void runQuestion(turn.question)}
                        onClarify={(choice) => void runQuestion(choice)}
                      />
                    ) : null}
                  </div>
                </article>
              ))}
              <div ref={timelineEndRef} />
            </>
          )}
        </div>

        <div className="border-t border-border p-3">
          <QuestionInput
            value={draft}
            onChange={setDraft}
            streaming={streaming}
            onRun={(q) => void runQuestion(q)}
            onStop={() => void stopCurrentRun()}
          />
        </div>
      </main>
    </div>
  );
}
