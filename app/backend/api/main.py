from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .agent import router as agent_router
from .auth import router as auth_router, seed_admin
from .catalog import router as catalog_router
from .db import init_db
from .history import router as history_router
from .metrics import router as metrics_router
from .obs_metrics import observe_http, router as obs_router, start_etl_refresher
from .ops import router as ops_router
from .pipelines import router as pipelines_router
from .settings import get_settings
from .settings_routes import router as settings_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    init_db()
    seed_admin()
    start_etl_refresher()
    yield


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
        allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )

    @app.middleware("http")
    async def request_id(request: Request, call_next):
        request_id_value = request.headers.get("x-request-id") or str(uuid.uuid4())
        started = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - started
        response.headers["X-Request-ID"] = request_id_value
        # Prometheus HTTP metrics. Use the route template (e.g.
        # /agent/sessions/{session_id}) instead of the raw path to keep label
        # cardinality bounded. Never let instrumentation break the request.
        try:
            route = request.scope.get("route")
            path_label = getattr(route, "path", None) or request.url.path
            observe_http(request.method, path_label, response.status_code, elapsed)
        except Exception:
            pass
        logging.getLogger("agent4da.api").info(
            "%s %s -> %s %.2fms",
            request.method,
            request.url.path,
            response.status_code,
            elapsed * 1000,
        )
        return response

    @app.get("/", tags=["meta"])
    def root() -> dict[str, Any]:
        return {"app": settings.app_name, "version": settings.app_version, "docs": "/docs"}

    @app.get("/healthz", tags=["meta"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["meta"])
    def readyz() -> dict[str, str]:
        return {"status": "ready"}

    app.include_router(auth_router)
    app.include_router(settings_router)
    app.include_router(agent_router)
    app.include_router(history_router)
    app.include_router(catalog_router)
    app.include_router(metrics_router)
    app.include_router(pipelines_router)
    app.include_router(ops_router)
    app.include_router(obs_router)  # Prometheus scrape endpoint: GET /metrics
    return app


app = create_app()

