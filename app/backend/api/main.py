"""FastAPI entrypoint for the Agent4DA Analytics Console backend.

Phase 1 surface:
- GET /healthz   - liveness probe used by docker-compose and the topbar boot.
- GET /readyz    - readiness probe (same as healthz until DB lands in Phase 2).
- GET /          - tiny banner so a human hitting the root sees something useful.
- /openapi.json  - exposed automatically by FastAPI.

Later phases mount routers under /auth, /agent, /history, /catalog,
/pipelines, /settings, /ops, /quickstats. They live in sibling packages and
are wired here.
"""

from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.logging import configure_logging
from api.settings import get_settings
from agent.router import router as agent_router
from auth.router import router as auth_router
from catalog.router import router as catalog_router
from history.router import router as history_router
from ops.health import router as ops_router
from ops.scheduler import start_scheduler, stop_scheduler
from pipelines.router import router as pipelines_router
from quickstats.router import router as quickstats_router
from settings_router.router import router as settings_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.app_env)
    log = structlog.get_logger("api.lifespan")
    log.info(
        "backend.starting",
        app_name=settings.app_name,
        env=settings.app_env,
        version=settings.app_version,
        cors_origins=settings.cors_origins_list,
    )
    start_scheduler()
    yield
    stop_scheduler()
    log.info("backend.stopping")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        start = time.perf_counter()
        log = structlog.get_logger("api.request").bind(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )
        try:
            response = await call_next(request)
        except Exception:
            log.exception("api.request.unhandled")
            raise
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        log.info("api.request.done", status_code=response.status_code, duration_ms=duration_ms)
        return response

    @app.get("/", tags=["meta"])
    def root() -> Dict[str, Any]:
        return {
            "app": settings.app_name,
            "version": settings.app_version,
            "env": settings.app_env,
            "docs": "/docs",
            "openapi": "/openapi.json",
        }

    @app.get("/healthz", tags=["meta"])
    def healthz() -> Dict[str, str]:
        # Liveness only. Returns 200 as long as the process is alive.
        return {"status": "ok"}

    @app.get("/readyz", tags=["meta"])
    def readyz() -> JSONResponse:
        # Readiness. In Phase 1 we only require process boot; later phases will
        # check DB connectivity, Trino reachability, etc.
        return JSONResponse({"status": "ready"}, status_code=200)

    # Phase 2 routers.
    app.include_router(auth_router)
    app.include_router(settings_router)

    # Phase 3 routers.
    app.include_router(agent_router)

    # Phase 4 routers.
    app.include_router(history_router)

    # Phase 5 routers.
    app.include_router(catalog_router)
    app.include_router(quickstats_router)

    # Phase 6 routers.
    app.include_router(pipelines_router)

    # Phase 7 routers.
    app.include_router(ops_router)

    return app


app = create_app()
