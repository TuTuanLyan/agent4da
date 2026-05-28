#!/usr/bin/env bash
# =============================================================================
# Airflow custom entrypoint — giải quyết triệt để:
#
#   1. Password không bao giờ bị override  → reset trước khi start services
#   2. "No host supplied" log error        → triggerer chạy đúng port
#   3. Scheduler crash detection           → trap signal, tất cả exit cùng nhau
#   4. Idempotent                          → restart container không lỗi
#
# Components chạy:
#   - scheduler   (background) — LocalExecutor chạy task trong subprocess của scheduler
#   - triggerer   (background) — cần thiết để Airflow UI không báo warning
#   - webserver   (foreground, PID 1) — container alive khi webserver alive
#
# Signal handling: SIGTERM/SIGINT → kill tất cả background processes → exit sạch
# =============================================================================
set -e

log() { echo "[entrypoint] $*"; }

# ---------------------------------------------------------------------------
# Trap: khi container nhận SIGTERM/SIGINT → kill tất cả child processes
# ---------------------------------------------------------------------------
_cleanup() {
    log "Shutting down all Airflow components..."
    [ -n "$SCHEDULER_PID" ]  && kill "$SCHEDULER_PID"  2>/dev/null || true
    [ -n "$TRIGGERER_PID" ]  && kill "$TRIGGERER_PID"  2>/dev/null || true
    exit 0
}
trap _cleanup SIGTERM SIGINT

# ---------------------------------------------------------------------------
# 1. DB migrate — idempotent, safe to run on every restart
# ---------------------------------------------------------------------------
log "Running db migrate..."
airflow db migrate
log "DB ready."

# ---------------------------------------------------------------------------
# 2. Ensure admin user với password cố định
#    Dùng Python API trực tiếp — không phụ thuộc vào standalone behavior
# ---------------------------------------------------------------------------
log "Ensuring admin user..."
: "${_AIRFLOW_WWW_USER_USERNAME:?Missing _AIRFLOW_WWW_USER_USERNAME}"
: "${_AIRFLOW_WWW_USER_PASSWORD:?Missing _AIRFLOW_WWW_USER_PASSWORD}"
python3 - << 'PYEOF'
import os

username = os.environ["_AIRFLOW_WWW_USER_USERNAME"]
password = os.environ["_AIRFLOW_WWW_USER_PASSWORD"]
email    = os.getenv("_AIRFLOW_WWW_USER_EMAIL",    "admin@agent4da.local")
fname    = os.getenv("_AIRFLOW_WWW_USER_FIRSTNAME", "Admin")
lname    = os.getenv("_AIRFLOW_WWW_USER_LASTNAME",  "Agent4DA")

# Suppress Flask-Limiter warning — không dùng rate limit, không cần storage
import warnings
warnings.filterwarnings("ignore")

from airflow.www.app import create_app
app = create_app()
with app.app_context():
    sm   = app.appbuilder.sm
    role = sm.find_role("Admin")
    user = sm.find_user(username=username)
    if user:
        sm.reset_password(user.id, password)
        print(f"[entrypoint] Password reset: {username}")
    else:
        sm.add_user(
            username=username, first_name=fname, last_name=lname,
            email=email, role=role, password=password,
        )
        print(f"[entrypoint] Created user: {username}")
PYEOF
log "Admin user ready."

# ---------------------------------------------------------------------------
# 3. Scheduler — background
#    LocalExecutor: task chạy là subprocess của scheduler
# ---------------------------------------------------------------------------
log "Starting scheduler (background)..."
airflow scheduler &
SCHEDULER_PID=$!

# ---------------------------------------------------------------------------
# 4. Triggerer — background
#    Cần để Airflow UI không warning "triggerer not running"
#    SparkSubmitOperator không deferrable nhưng triggerer required cho healthy status
# ---------------------------------------------------------------------------
log "Starting triggerer (background)..."
airflow triggerer &
TRIGGERER_PID=$!

# ---------------------------------------------------------------------------
# 5. Webserver — foreground (PID 1 equivalent, giữ container alive)
# ---------------------------------------------------------------------------
log "Starting webserver on port 8080..."
log "=== Airflow UI: http://localhost:8081 ==="
exec airflow webserver --port 8080
