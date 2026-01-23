# burningdemand_dagster/resources/http_clients_resource.py
import asyncio
import logging
import random
import threading
import time
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse

import httpx
from aiolimiter import AsyncLimiter
from dagster import ConfigurableResource
from pydantic import Field

_RETRYABLE = {429, 500, 502, 503, 504}

# Rate limits per domain (requests per minute)
_RATE_LIMITS = {
    "api.github.com": 30,  # GitHub search API: 30 requests/minute
    # Add other domains as needed
}

logger = logging.getLogger(__name__)

# Global rate limiters per domain (async uses aiolimiter, sync uses simple time-based)
_rate_limiters_async: dict[str, AsyncLimiter] = {}
_rate_limiters_sync: dict[str, dict] = defaultdict(lambda: {"last_request": 0.0, "lock": threading.Lock()})


def _safe_async_close(coro):
    """Close an async resource from a sync context."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        asyncio.run(coro)


def _get_response_body(exc: Exception) -> str:
    """Extract response body from HTTPStatusError."""
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            return exc.response.text
        except Exception:
            pass
    return ""


def _get_rate_limiter_async(domain: str) -> Optional[AsyncLimiter]:
    """Get or create async rate limiter for a domain."""
    if domain not in _RATE_LIMITS:
        return None
    
    if domain not in _rate_limiters_async:
        requests_per_minute = _RATE_LIMITS[domain]
        # aiolimiter uses max_rate (requests per second), so convert from per-minute
        requests_per_second = requests_per_minute / 60.0
        _rate_limiters_async[domain] = AsyncLimiter(max_rate=requests_per_second, time_period=1.0)
    
    return _rate_limiters_async[domain]


def _wait_for_rate_limit_sync(url: str) -> None:
    """Wait if necessary to respect rate limits for the domain (sync version)."""
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.hostname or ""
    
    if domain not in _RATE_LIMITS:
        return
    
    requests_per_minute = _RATE_LIMITS[domain]
    min_delay = 60.0 / requests_per_minute
    
    limiter = _rate_limiters_sync[domain]
    with limiter["lock"]:
        current_time = time.time()
        time_since_last = current_time - limiter["last_request"]
        
        if time_since_last < min_delay:
            wait_time = min_delay - time_since_last
            time.sleep(wait_time)
        
        limiter["last_request"] = time.time()


class HTTPClientsResource(ConfigurableResource):
    user_agent: str = Field(default="BurningDemand/0.1")

    def setup_for_execution(self, context) -> None:
        timeout = httpx.Timeout(30.0)
        headers = {"User-Agent": self.user_agent}
        self._client = httpx.Client(timeout=timeout, headers=headers)
        self._aclient = httpx.AsyncClient(timeout=timeout, headers=headers)
        self._context = context  # Store context for logging

    def teardown_after_execution(self, context) -> None:
        try:
            self._client.close()
        except Exception:
            pass
        try:
            _safe_async_close(self._aclient.aclose())
        except Exception:
            pass
        self._context = None

    @property
    def client(self) -> httpx.Client:
        return self._client

    @property
    def aclient(self) -> httpx.AsyncClient:
        return self._aclient

    async def request_with_retry_async(
        self,
        method: str,
        url: str,
        *,
        max_attempts: int = 1,
        **kwargs,
    ) -> httpx.Response:
        """Make an async HTTP request with automatic retry logic and rate limiting."""
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.hostname or ""
        limiter = _get_rate_limiter_async(domain)
        
        last_exc: Optional[Exception] = None
        for attempt in range(max_attempts):
            try:
                # Wait for rate limit before making request (using aiolimiter)
                if limiter:
                    async with limiter:
                        resp = await self._aclient.request(method, url, **kwargs)
                else:
                    resp = await self._aclient.request(method, url, **kwargs)
                
                if resp.status_code in _RETRYABLE:
                    wait = min(30.0, (2**attempt) + random.random())
                    msg = f"Retryable {resp.status_code} for {method} {url}, attempt {attempt + 1}/{max_attempts}"
                    if hasattr(self, "_context") and self._context:
                        self._context.log.warning(msg)
                    else:
                        logger.warning(msg)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except Exception as e:
                last_exc = e
                wait = min(30.0, (2**attempt) + random.random())
                msg = f"Request failed {method} {url}, attempt {attempt + 1}/{max_attempts}: {e}"
                if hasattr(self, "_context") and self._context:
                    self._context.log.warning(msg)
                else:
                    logger.warning(msg)
                if body := _get_response_body(e):
                    body_msg = f"Response body: {body}"
                    if hasattr(self, "_context") and self._context:
                        self._context.log.warning(body_msg)
                    else:
                        logger.warning(body_msg)
                await asyncio.sleep(wait)
        error_msg = f"All {max_attempts} attempts failed for {method} {url}: {last_exc}"
        if hasattr(self, "_context") and self._context:
            self._context.log.error(error_msg)
        else:
            logger.error(error_msg)
        if body := _get_response_body(last_exc):
            body_msg = f"Response body: {body}"
            if hasattr(self, "_context") and self._context:
                self._context.log.error(body_msg)
            else:
                logger.error(body_msg)
        raise last_exc or RuntimeError("request_with_retry_async failed")

    def request_with_retry(
        self,
        method: str,
        url: str,
        *,
        max_attempts: int = 6,
        **kwargs,
    ) -> httpx.Response:
        """Make a sync HTTP request with automatic retry logic and rate limiting."""
        last_exc: Optional[Exception] = None
        for attempt in range(max_attempts):
            try:
                # Wait for rate limit before making request (sync version)
                _wait_for_rate_limit_sync(url)
                resp = self._client.request(method, url, **kwargs)
                if resp.status_code in _RETRYABLE:
                    wait = min(30.0, (2**attempt) + random.random())
                    msg = f"Retryable {resp.status_code} for {method} {url}, attempt {attempt + 1}/{max_attempts}"
                    if hasattr(self, "_context") and self._context:
                        self._context.log.warning(msg)
                    else:
                        logger.warning(msg)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except Exception as e:
                last_exc = e
                wait = min(30.0, (2**attempt) + random.random())
                msg = f"Request failed {method} {url}, attempt {attempt + 1}/{max_attempts}: {e}"
                if hasattr(self, "_context") and self._context:
                    self._context.log.warning(msg)
                else:
                    logger.warning(msg)
                if body := _get_response_body(e):
                    body_msg = f"Response body: {body}"
                    if hasattr(self, "_context") and self._context:
                        self._context.log.warning(body_msg)
                    else:
                        logger.warning(body_msg)
                time.sleep(wait)
        error_msg = f"All {max_attempts} attempts failed for {method} {url}: {last_exc}"
        if hasattr(self, "_context") and self._context:
            self._context.log.error(error_msg)
        else:
            logger.error(error_msg)
        if body := _get_response_body(last_exc):
            body_msg = f"Response body: {body}"
            if hasattr(self, "_context") and self._context:
                self._context.log.error(body_msg)
            else:
                logger.error(body_msg)
        raise last_exc or RuntimeError("request_with_retry failed")
