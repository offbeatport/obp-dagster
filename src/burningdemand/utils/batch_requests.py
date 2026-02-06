import asyncio
import time
from typing import Dict, List, Optional

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
    context: AssetExecutionContext,
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

    async def run_request(idx: int, req: dict) -> tuple[int, httpx.Response]:
        await limiter.try_acquire_async("request")
        last_err: Optional[Exception] = None
        for attempt in range(CONNECT_RETRY_ATTEMPTS):
            try:
                resp = await client.request(**req)
                resp.raise_for_status()
                return idx, resp
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                last_err = e
                if attempt < CONNECT_RETRY_ATTEMPTS - 1:
                    delay = CONNECT_RETRY_BACKOFF_SEC[attempt]
                    context.log.warning(
                        f"Connection error (attempt {attempt + 1}/{CONNECT_RETRY_ATTEMPTS}): {e}. Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    context.log.error(
                        f"Connection failed after {CONNECT_RETRY_ATTEMPTS} attempts. "
                        "Check network, DNS, firewall/proxy, and that the target host is reachable."
                    )
                    raise
        assert last_err is not None
        raise last_err

    tasks = [
        asyncio.create_task(run_request(idx, req)) for idx, req in enumerate(requests)
    ]
    results: Dict[int, httpx.Response] = {}
    for task in asyncio.as_completed(tasks):
        idx, resp = await task
        results[idx] = resp

    ordered = [results[i] for i in range(n)]
    elapsed = time.monotonic() - start
    context.log.info(f"Batch completed: {n} requests in {elapsed:.2f}s")
    return ordered
