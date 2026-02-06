import asyncio
import pprint
import time
from typing import Any, Dict, List, Optional

import httpx
from dagster import AssetExecutionContext
from pyrate_limiter import Duration, Limiter
from pyrate_limiter.limiter_factory import create_inmemory_limiter

# 30 requests per minute when caller does not pass a limiter
DEFAULT_LIMITER = create_inmemory_limiter(30, Duration.MINUTE)

# Retry on connection failures (transient network/DNS/TLS issues)
CONNECT_RETRY_ATTEMPTS = 3
CONNECT_RETRY_BACKOFF_SEC = [2.0, 4.0, 8.0]


async def batch_requests(
    client: httpx.AsyncClient,
    context: Any,
    requests: List[dict],
    limiter: Optional[Limiter] = DEFAULT_LIMITER,
) -> List[httpx.Response]:
    """
    Fire all requests concurrently.
    Only wait when the rate limiter enforces it.
    Retries on connection errors (ConnectError, ConnectTimeout).
    Responses are returned in request order.
    """
    if not requests:
        return []

    n = len(requests)
    start = time.monotonic()
    context.log.info(f"Batch requested: {n} requests (launching concurrently)")

    def _truncate(s: str, limit: int = 10 * 1000) -> str:
        if len(s) <= limit:
            return s
        return s[:limit] + "... [truncated]"

    def _safe_response_body(resp: httpx.Response) -> str:
        try:
            return _truncate(resp.text)
        except Exception as e:  # pragma: no cover
            return f"<unreadable body: {e!r}>"

    async def run_request(idx: int, req: dict) -> tuple[int, Optional[httpx.Response]]:
        if limiter is not None:
            await limiter.try_acquire_async("request")
        last_err: Optional[Exception] = None
        for attempt in range(CONNECT_RETRY_ATTEMPTS):
            try:
                resp = await client.request(**req)
                return idx, resp
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                last_err = e
                if attempt < CONNECT_RETRY_ATTEMPTS - 1:
                    delay = CONNECT_RETRY_BACKOFF_SEC[attempt]
                    context.log.error(
                        "Connection error for request "
                        f"[idx={idx}, method={req.get('method')}, url={req.get('url')}] "
                        f"(attempt {attempt + 1}/{CONNECT_RETRY_ATTEMPTS}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    context.log.error(
                        "Request failed after %s attempts "
                        "[idx=%s, method=%s, url=%s]. "
                        "Last error: %r",
                        CONNECT_RETRY_ATTEMPTS,
                        idx,
                        req.get("method"),
                        req.get("url"),
                        last_err,
                        exc_info=True,
                    )
                    # Return a sentinel None for this index instead of failing the whole batch.
                    return idx, None
        # Should not reach here, but keep mypy/pyright happy.
        return idx, None

    tasks = [
        asyncio.create_task(run_request(idx, req)) for idx, req in enumerate(requests)
    ]
    results: Dict[int, httpx.Response] = {}
    completed = 0
    for task in asyncio.as_completed(tasks):
        idx, resp = await task
        completed += 1
        if resp is not None:
            context.log.info(
                "Request %s/%s completed [idx=%s, status=%s]",
                completed,
                n,
                idx,
                resp.status_code,
            )
            if resp.is_error:
                req = requests[idx]
                context.log.warning(
                    "Request returned error status [idx=%s, status=%s, method=%s, url=%s]. Body: %s",
                    idx,
                    resp.status_code,
                    req.get("method"),
                    req.get("url"),
                    _safe_response_body(resp),
                )
            results[idx] = resp
        else:
            req = requests[idx]
            context.log.warning(
                "Request %s/%s failed; omitting [idx=%s, method=%s, url=%s]",
                completed,
                n,
                idx,
                req.get("method"),
                req.get("url"),
            )

    # Preserve request order for successful responses; failed ones are omitted.
    ordered = [results[i] for i in range(n) if i in results]
    elapsed = time.monotonic() - start
    context.log.info(f"Batch completed: {n} requests in {elapsed:.2f}s")
    return ordered
