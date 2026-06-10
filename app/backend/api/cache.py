"""Redis cache + rate-limit layer for the Agent4DA backend.

Design goals
------------
* **Optional / graceful.** If the `redis` package is missing, `APP_REDIS_URL`
  is unset, or Redis is simply down, every function in this module degrades to a
  safe no-op: cache reads return ``None`` (a miss), cache writes do nothing, and
  the rate limiter *fails open* (always allows). The app then behaves exactly as
  it did before Redis existed - just without the speedup. A Redis outage must
  never take the app down or lock users out.
* **Cheap to call.** Connection attempts use short timeouts and, after a
  failure, are not retried again for ``RECONNECT_INTERVAL_S`` seconds so a dead
  Redis doesn't add latency to every request.
* **All Redis usage lives here** so the rest of the codebase imports a handful of
  small helpers and never touches a raw client.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Optional

from .settings import get_settings

log = logging.getLogger("agent4da.cache")

KEY_PREFIX = "a4da"
RECONNECT_INTERVAL_S = 30.0

# Module-level connection state. `_client is None` + `_disabled_until > now`
# means "we tried recently and failed; don't hammer it".
_client: Any = None
_initialized = False
_disabled_until = 0.0


def _redis_url() -> str:
    return get_settings().redis_url


def _get_client():
    """Return a live redis client, or ``None`` if Redis is unavailable.

    Never raises. Caches both success and (temporarily) failure.
    """
    global _client, _initialized, _disabled_until

    settings = get_settings()
    if not settings.cache_enabled or not settings.redis_url:
        return None

    if _client is not None:
        return _client

    now = time.monotonic()
    if _initialized and now < _disabled_until:
        # Recently failed; stay quiet until the cool-down passes.
        return None

    try:
        import redis  # local import so the dependency stays optional

        client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=0.25,
            socket_timeout=0.25,
            health_check_interval=30,
        )
        client.ping()
        _client = client
        _initialized = True
        log.info("Redis cache connected: %s", settings.redis_url)
        return _client
    except Exception as exc:  # noqa: BLE001 - any failure -> graceful disable
        _client = None
        _initialized = True
        _disabled_until = now + RECONNECT_INTERVAL_S
        log.warning(
            "Redis unavailable (%s). Caching disabled for %ss.",
            f"{type(exc).__name__}: {exc}",
            int(RECONNECT_INTERVAL_S),
        )
        return None


def _mark_dead() -> None:
    """Drop the client after a runtime error so the next call reconnects."""
    global _client, _disabled_until
    _client = None
    _disabled_until = time.monotonic() + RECONNECT_INTERVAL_S


def is_available() -> bool:
    return _get_client() is not None


def health() -> dict:
    """Lightweight status for /readyz. Never raises."""
    settings = get_settings()
    if not settings.cache_enabled:
        return {"enabled": False, "connected": False, "detail": "cache disabled by config"}
    client = _get_client()
    if client is None:
        return {"enabled": True, "connected": False, "detail": "redis unreachable"}
    try:
        client.ping()
        return {"enabled": True, "connected": True}
    except Exception as exc:  # noqa: BLE001
        _mark_dead()
        return {"enabled": True, "connected": False, "detail": f"{type(exc).__name__}: {exc}"}


# --------------------------------------------------------------------------- #
# Key helpers
# --------------------------------------------------------------------------- #
def make_key(*parts: Any) -> str:
    """Build a namespaced key from string parts."""
    return ":".join([KEY_PREFIX, *(str(p) for p in parts)])


def fingerprint(*parts: Any) -> str:
    """Stable short hash of arbitrary parts (dicts allowed) for cache keys."""
    blob = json.dumps(parts, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------- #
# JSON cache get / set / delete  (all no-op + None on any failure)
# --------------------------------------------------------------------------- #
def get_json(key: str) -> Optional[Any]:
    client = _get_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
    except Exception as exc:  # noqa: BLE001
        log.debug("cache get failed: %s", exc)
        _mark_dead()
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def set_json(key: str, value: Any, ttl_seconds: int) -> bool:
    client = _get_client()
    if client is None:
        return False
    try:
        payload = json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as exc:
        log.debug("cache set skipped (not JSON-serializable): %s", exc)
        return False
    try:
        client.set(key, payload, ex=max(1, int(ttl_seconds)))
        return True
    except Exception as exc:  # noqa: BLE001
        log.debug("cache set failed: %s", exc)
        _mark_dead()
        return False


def delete(*keys: str) -> None:
    if not keys:
        return
    client = _get_client()
    if client is None:
        return
    try:
        client.delete(*keys)
    except Exception as exc:  # noqa: BLE001
        log.debug("cache delete failed: %s", exc)
        _mark_dead()


# --------------------------------------------------------------------------- #
# Rate limiting  (fixed window; FAIL-OPEN if Redis is down)
# --------------------------------------------------------------------------- #
def rate_limit_check(bucket: str, identifier: str, limit: int, window_seconds: int) -> dict:
    """Increment a fixed-window counter and report whether the call is allowed.

    Returns ``{"allowed": bool, "remaining": int, "retry_after": int}``.

    If Redis is unavailable the call is **allowed** (fail-open): we never block
    legitimate users because the cache is down.
    """
    if limit <= 0:
        return {"allowed": True, "remaining": -1, "retry_after": 0}

    client = _get_client()
    if client is None:
        return {"allowed": True, "remaining": -1, "retry_after": 0}

    key = make_key("rl", bucket, identifier)
    try:
        pipe = client.pipeline()
        pipe.incr(key, 1)
        pipe.ttl(key)
        count, ttl = pipe.execute()
        count = int(count)
        if count == 1 or ttl is None or ttl < 0:
            client.expire(key, max(1, int(window_seconds)))
            ttl = window_seconds
        if count > limit:
            return {"allowed": False, "remaining": 0, "retry_after": int(ttl)}
        return {"allowed": True, "remaining": max(0, limit - count), "retry_after": 0}
    except Exception as exc:  # noqa: BLE001
        log.debug("rate limit check failed (fail-open): %s", exc)
        _mark_dead()
        return {"allowed": True, "remaining": -1, "retry_after": 0}


def reset_for_tests() -> None:
    """Test hook: forget cached client/connection state."""
    global _client, _initialized, _disabled_until
    _client = None
    _initialized = False
    _disabled_until = 0.0
