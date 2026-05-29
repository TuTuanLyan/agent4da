# syntax=docker/dockerfile:1.7
# Multi-stage build for the Agent4DA Analytics Console backend.
# Stage 1 builds wheels, stage 2 runs the slim image.

FROM python:3.12-slim AS builder

WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --upgrade pip setuptools wheel \
 && pip install --prefix=/install \
        "fastapi>=0.115,<1" \
        "uvicorn[standard]>=0.30,<1" \
        "pydantic>=2.7,<3" \
        "pydantic-settings>=2.4,<3" \
        "pydantic[email]>=2.7,<3" \
        "structlog>=24.1,<26" \
        "python-dotenv>=1.0,<2" \
        "sqlalchemy>=2.0,<3" \
        "alembic>=1.13,<2" \
        "psycopg[binary]>=3.2,<4" \
        "argon2-cffi>=23.1,<26" \
        "pyjwt>=2.9,<3" \
        "python-multipart>=0.0.9,<1" \
        "langgraph>=1.0,<2" \
        "langchain-core>=1.0,<2" \
        "openai>=1.40,<3" \
        "packaging>=24.0,<27" \
        "sqlglot>=25.0,<28" \
        "trino>=0.328,<1" \
        "httpx>=0.27,<1" \
        "apscheduler>=3.10,<4" \
        "minio>=7.2,<8" \
        "pyarrow>=16.0,<23" \
 && pip install --prefix=/install --ignore-installed "packaging>=24.0,<27"

# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/project/app/backend:/opt/project/code/agent

WORKDIR /opt/project/app/backend

COPY --from=builder /install /usr/local
COPY . /opt/project/app/backend

# The compose file mounts /opt/project/code:ro from the host repo so the
# backend can `from graph.sql_graph import graph` in later phases.
RUN mkdir -p /opt/project/code

# Make sure the entrypoint is readable AND executable for the non-root
# `app` user regardless of host umask. macOS umask sometimes produces 711
# (no read for others), which makes shebang scripts fail with
# "cannot open" since the shell can't read the script to interpret it.
RUN chmod 0755 /opt/project/app/backend/scripts/entrypoint.sh

RUN useradd --create-home --uid 1001 app
USER app

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=20s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/healthz', timeout=2).status==200 else 1)" || exit 1

ENTRYPOINT ["/opt/project/app/backend/scripts/entrypoint.sh"]
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
