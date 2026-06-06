"""Tests for the Redis cache + rate-limit layer (api/cache.py).

Two things are proven without needing a real Redis server:

1. **Graceful degradation** - when Redis is unavailable, reads miss, writes are
   no-ops, and the rate limiter fails OPEN (never blocks). This is the property
   that keeps the app working when Redis is down.
2. **Functional behaviour** - with a tiny in-memory stand-in injected as the
   client, get/set/delete round-trip correctly and the fixed-window rate limiter
   blocks once the limit is exceeded.

Run:
    cd app/backend
    python tests/test_redis_cache.py        # or: python -m pytest tests/test_redis_cache.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import cache  # noqa: E402


class _Settings:
    def __init__(self, enabled=True, url="redis://127.0.0.1:6390/0"):
        self.cache_enabled = enabled
        self.redis_url = url


class _MiniPipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def incr(self, key, amount=1):
        self.ops.append(("incr", key, amount))
        return self

    def ttl(self, key):
        self.ops.append(("ttl", key))
        return self

    def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "incr":
                _, key, amount = op
                cur = int(self.r.store.get(key, 0)) + amount
                self.r.store[key] = cur
                out.append(cur)
            elif op[0] == "ttl":
                out.append(self.r.expiry.get(op[1], -2))
        self.ops = []
        return out


class _MiniRedis:
    """Just enough of the redis-py surface that cache.py uses."""

    def __init__(self):
        self.store = {}
        self.expiry = {}

    def ping(self):
        return True

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value
        if ex is not None:
            self.expiry[key] = ex
        return True

    def delete(self, *keys):
        n = 0
        for key in keys:
            n += 1 if self.store.pop(key, None) is not None else 0
            self.expiry.pop(key, None)
        return n

    def expire(self, key, seconds):
        self.expiry[key] = seconds
        return True

    def pipeline(self):
        return _MiniPipe(self)


# --------------------------------------------------------------------------- #
def test_make_key_and_fingerprint():
    assert cache.make_key("ans", "v1", "u", "s") == "a4da:ans:v1:u:s"
    fp1 = cache.fingerprint("doanh thu", "auto", {"last_sql": "x"})
    fp2 = cache.fingerprint("doanh thu", "auto", {"last_sql": "x"})
    fp3 = cache.fingerprint("doanh thu", "auto", {"last_sql": "y"})
    assert fp1 == fp2, "same inputs must hash the same"
    assert fp1 != fp3, "different context must hash differently"
    assert len(fp1) == 16


def test_graceful_when_unavailable():
    """Redis pointed at a dead port (or library missing) -> safe no-ops."""
    cache.reset_for_tests()
    original = cache.get_settings
    cache.get_settings = lambda: _Settings(enabled=True, url="redis://127.0.0.1:6390/0")
    try:
        assert cache.get_json("a4da:nope") is None
        assert cache.set_json("a4da:nope", {"x": 1}, 10) is False
        # rate limiter must FAIL OPEN when Redis is down
        verdict = cache.rate_limit_check("ask", "user-1", limit=1, window_seconds=60)
        assert verdict["allowed"] is True
        assert cache.is_available() is False
    finally:
        cache.get_settings = original
        cache.reset_for_tests()


def test_disabled_by_config():
    cache.reset_for_tests()
    original = cache.get_settings
    cache.get_settings = lambda: _Settings(enabled=False)
    try:
        assert cache.get_json("a4da:x") is None
        assert cache.set_json("a4da:x", 1, 10) is False
        assert cache.health()["enabled"] is False
    finally:
        cache.get_settings = original
        cache.reset_for_tests()


def test_functional_with_injected_client():
    cache.reset_for_tests()
    original = cache.get_settings
    cache.get_settings = lambda: _Settings(enabled=True, url="redis://localhost:6379/0")
    cache._client = _MiniRedis()
    cache._initialized = True
    cache._disabled_until = 0.0
    try:
        # round trip
        assert cache.set_json("a4da:k", {"hello": "world", "n": 3}, 30) is True
        assert cache.get_json("a4da:k") == {"hello": "world", "n": 3}
        # delete
        cache.delete("a4da:k")
        assert cache.get_json("a4da:k") is None
        # health
        assert cache.health() == {"enabled": True, "connected": True}
        # rate limit: limit=2 -> 3rd call blocked
        v1 = cache.rate_limit_check("ask", "u", limit=2, window_seconds=60)
        v2 = cache.rate_limit_check("ask", "u", limit=2, window_seconds=60)
        v3 = cache.rate_limit_check("ask", "u", limit=2, window_seconds=60)
        assert v1["allowed"] and v2["allowed"], "first two within limit"
        assert v3["allowed"] is False, "third over the limit must be blocked"
        assert v3["retry_after"] > 0
    finally:
        cache.get_settings = original
        cache.reset_for_tests()


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL  {name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"ERROR {name}: {type(exc).__name__}: {exc}")
    print("-" * 60)
    print("ALL PASSED" if failures == 0 else f"{failures} FAILURE(S)")
    sys.exit(1 if failures else 0)
