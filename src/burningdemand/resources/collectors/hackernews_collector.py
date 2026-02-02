"""Hacker News stories collector."""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx
from dagster import ConfigurableResource
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.utils.batch_requests import batch_requests
from burningdemand.utils.url import iso_date_to_utc_bounds
from .queries import get_body_max_length, matches_query_keywords


# 100 requests per minute (shared across collect() calls)
RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=100,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_hn.db",
    use_file_lock=True,
)


class HackerNewsCollector(ConfigurableResource):
    """Collector for Hacker News stories."""

    def setup_for_execution(self, context) -> None:
        self._context = context
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={"User-Agent": "BurningDemand/0.1"},
        )

    def teardown_after_execution(self, context) -> None:
        if hasattr(self, "_client") and self._client is not None:
            try:
                asyncio.run(self._client.aclose())
            except Exception:
                pass
            self._client = None

    async def collect(self, date: str) -> Tuple[List[Dict], Dict]:
        """Collect Hacker News stories for the given date."""
        from_ts, to_ts = iso_date_to_utc_bounds(date)

        self._context.log.info("HackerNews: 10 pages")
        specs = [
            {
                "method": "GET",
                "url": "https://hn.algolia.com/api/v1/search_by_date",
                "params": {
                    "tags": "story",
                    "numericFilters": f"created_at_i>{from_ts},created_at_i<{to_ts}",
                    "hitsPerPage": 100,
                    "page": page,
                },
            }
            for page in range(10)
        ]

        responses = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )

        pages = [resp.json().get("hits", []) for resp in responses]
        items = []
        for hits in pages:
            for it in hits:
                title = it.get("title") or ""
                body = it.get("story_text") or ""
                if matches_query_keywords(f"{title} {body}", "hackernews"):
                    created_i = int(it.get("created_at_i") or 0)
                    items.append(
                        {
                            "url": it.get("url")
                            or f"https://news.ycombinator.com/item?id={it.get('objectID')}",
                            "title": title,
                            "body": body[: get_body_max_length()],
                            "created_at": (
                                datetime.fromtimestamp(
                                    created_i, tz=timezone.utc
                                ).isoformat()
                                if created_i
                                else ""
                            ),
                            "comment_count": it.get("num_comments", 0) or 0,
                            "vote_count": it.get("points", 0) or 0,
                        }
                    )

        self._context.log.info(
            f"HackerNews: {len(responses)} requests, {len(items)} items"
        )
        return items, {"requests": len(responses)}
