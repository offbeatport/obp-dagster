import asyncio
import time
from collections import defaultdict
from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple

import httpx
from dagster import AssetExecutionContext

# Rate limit configuration: (max_requests, time_period_seconds)
RATE_LIMITS: Dict[str, Tuple[int, float]] = {
    "api.github.com": (30, 60.0),
    "api.stackexchange.com": (30, 60.0),
    "www.reddit.com": (60, 60.0),
    "oauth.reddit.com": (60, 60.0),
    "api.reddit.com": (60, 60.0),
    "hn.algolia.com": (100, 60.0),
}

# Per-domain monotonic timestamp when the next batch is allowed to start.
# This makes rate limiting global across multiple calls to batch_requests
# within the same process, so successive calls don't overlap time windows.
_NEXT_AVAILABLE_TIME: Dict[str, float] = {}


def create_async_client(
    timeout: float,
    user_agent: str = "BurningDemand/0.1",
) -> httpx.AsyncClient:
    """Create an async httpx client with connection pooling."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers={"User-Agent": user_agent},
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
    )


async def request_async(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Make an async HTTP request (rate limiting handled at batch level)."""
    resp = await client.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: list[dict],
) -> list[httpx.Response]:
    """Execute requests in strict batches to enforce rate limits.

    For domains with rate limits, ensures no more than ``rate_limit`` requests
    per ``time_period`` by batching: send ``rate_limit`` requests, then enforce
    at least ``time_period`` seconds between successive batches *per process*.
    """
    if not requests:
        return []

    context.log.info(f"Executing {len(requests)} requests with rate limiting")

    # Group requests by domain
    requests_by_domain: dict[str, list[tuple[int, dict]]] = defaultdict(list)
    for idx, req in enumerate(requests):
        domain = urlparse(req.get("url", "")).netloc or ""
        requests_by_domain[domain].append((idx, req))

    all_results: dict[int, httpx.Response] = {}

    for domain, domain_requests in requests_by_domain.items():
        rate_limit_config = RATE_LIMITS.get(domain)

        if rate_limit_config:
            rate_limit, time_period = rate_limit_config
            total = len(domain_requests)
            batch_num = 0

            # Get the earliest time we are allowed to start a batch for this domain
            next_available = _NEXT_AVAILABLE_TIME.get(domain, 0.0)

            for batch_start in range(0, total, rate_limit):
                batch_end = min(batch_start + rate_limit, total)
                batch = domain_requests[batch_start:batch_end]
                batch_num += 1

                # Enforce spacing between batches across calls in this process
                now = time.monotonic()
                if now < next_available:
                    wait_for = next_available - now
                    context.log.info(
                        f"{domain}: Waiting {wait_for:.2f}s before next batch "
                        f"to respect rate limit window..."
                    )
                    await asyncio.sleep(wait_for)

                context.log.info(
                    f"{domain}: Batch {batch_num} of {len(batch)} requests "
                    f"({batch_start + 1}-{batch_end} of {total})"
                )

                tasks = [request_async(client, **req) for idx, req in batch]
                batch_results = await asyncio.gather(*tasks)

                for (idx, _), resp in zip(batch, batch_results):
                    all_results[idx] = resp

                # After this batch completes, set the next allowed time for this domain
                next_available = time.monotonic() + time_period

            # Persist the next available time for this domain so subsequent calls
            # to batch_requests in this process also respect the same window.
            _NEXT_AVAILABLE_TIME[domain] = next_available
        else:
            # No rate limit: process all concurrently
            tasks = [request_async(client, **req) for idx, req in domain_requests]
            results = await asyncio.gather(*tasks)
            for (idx, _), resp in zip(domain_requests, results):
                all_results[idx] = resp

    results = [all_results[i] for i in range(len(requests))]
    context.log.info(f"Completed all {len(results)} requests")
    return results
