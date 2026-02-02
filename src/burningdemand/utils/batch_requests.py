import asyncio
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

    context.log.info(f"Executing {len(requests)} requests")

    async def run_request(idx: int, req: dict) -> tuple[int, httpx.Response]:
        # Only block here if rate limit is hit
        print(f"Aquire {idx}")
        await limiter.try_acquire_async()
        print(f"Trigger request {idx}")

        resp = await client.request(**req)
        print(f"Request Done request {idx}")
        resp.raise_for_status()
        print(f"Raise for status  request {idx}")
        return idx, resp

    # Launch all tasks immediately
    tasks = [
        asyncio.create_task(run_request(idx, req)) for idx, req in enumerate(requests)
    ]

    # Await completion (no ordering guarantees here)
    results: Dict[int, httpx.Response] = {}
    for task in asyncio.as_completed(tasks):
        idx, resp = await task
        results[idx] = resp

    # Reorder to match input request order
    ordered = [results[i] for i in range(len(requests))]
    context.log.info(f"Completed {len(ordered)} requests")
    return ordered
