import asyncio
import time
from typing import Dict, List, Optional

import httpx
from dagster import AssetExecutionContext
from pyrate_limiter import Duration, Limiter
from pyrate_limiter.limiter_factory import create_inmemory_limiter

# 30 requests per minute when caller does not pass a limiter
DEFAULT_LIMITER = create_inmemory_limiter(30, Duration.MINUTE)


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: List[dict],
    limiter: Optional[Limiter] = DEFAULT_LIMITER,
) -> List[httpx.Response]:
    """
    Fire all requests concurrently.
    Only wait when the rate limiter enforces it.
    Responses are returned in request order.
    """
    if not requests:
        return []

    n = len(requests)
    start = time.monotonic()

    async def run_request(idx: int, req: dict) -> tuple[int, httpx.Response]:
        await limiter.try_acquire_async("request")
        resp = await client.request(**req)
        resp.raise_for_status()
        return idx, resp

    tasks = [
        asyncio.create_task(run_request(idx, req)) for idx, req in enumerate(requests)
    ]
    results: Dict[int, httpx.Response] = {}
    for task in asyncio.as_completed(tasks):
        idx, resp = await task
        results[idx] = resp

    ordered = [results[i] for i in range(n)]
    elapsed = time.monotonic() - start
    context.log.info(f"{n} requests at once, batch took {elapsed:.2f}s")
    return ordered
