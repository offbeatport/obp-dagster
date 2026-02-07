"""
Rate limiters shared across runs. When REDIS_URL is set, uses Redis so all
workers/processes share the same limit per source. Otherwise uses in-memory
(per-process) limiters.
"""

import asyncio
import os
import time
from typing import Optional, Union

from pyrate_limiter import Duration, Limiter
from pyrate_limiter.limiter_factory import create_inmemory_limiter

# Optional Redis backend for cross-run sharing
try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


def _redis_url() -> Optional[str]:
    return os.environ.get("REDIS_URL")


class RedisAsyncRateLimiter:
    """
    Async rate limiter backed by Redis. Same key (source) is shared across
    all runs/workers, so e.g. 30/min for GitHub is enforced globally.
    """

    __slots__ = ("_source", "_limit", "_window_sec", "_redis", "_key_prefix")

    def __init__(self, source: str, limit: int, window_sec: float, redis_url: str):
        self._source = source
        self._limit = limit
        self._window_sec = window_sec
        self._key_prefix = f"rate_limit:{source}"
        self._redis = aioredis.from_url(redis_url, decode_responses=True)

    def _window_key(self) -> str:
        window_id = int(time.time() / self._window_sec)
        return f"{self._key_prefix}:{window_id}"

    async def try_acquire_async(
        self,
        name: str = "request",
        weight: int = 1,
        blocking: bool = True,
        timeout: int = -1,
    ) -> bool:
        while True:
            key = self._window_key()
            pipe = self._redis.pipeline()
            pipe.incr(key, weight)
            pipe.expire(key, int(self._window_sec) + 10)
            results = await pipe.execute()
            count = results[0]
            if count <= self._limit:
                return True
            if not blocking:
                return False
            # Wait until next window
            window_start = int(time.time() / self._window_sec) * self._window_sec
            next_start = window_start + self._window_sec
            wait_sec = next_start - time.time()
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)

    async def aclose(self) -> None:
        await self._redis.aclose()


# In-memory limiters per source when Redis is not used (not shared across runs)
_inmemory_limiters: dict[tuple[str, int, float], Limiter] = {}


def get_rate_limiter(
    source: str,
    limit: int,
    window_sec: float = 60.0,
) -> Union[Limiter, "RedisAsyncRateLimiter"]:
    """
    Return a rate limiter for the given source. When REDIS_URL is set, returns
    a Redis-backed limiter shared across all runs. Otherwise returns an
    in-memory limiter (per process only).
    """
    redis_url = _redis_url()
    if redis_url and aioredis is not None:
        return RedisAsyncRateLimiter(source, limit, window_sec, redis_url)
    # In-memory: cache per (source, limit, window) in this process
    key = (source, limit, window_sec)
    if key not in _inmemory_limiters:
        duration = Duration.SECOND * window_sec if window_sec >= 1 else Duration.MINUTE
        if window_sec != 60.0:
            duration = Duration(seconds=window_sec)
        _inmemory_limiters[key] = create_inmemory_limiter(limit, duration)
    return _inmemory_limiters[key]
