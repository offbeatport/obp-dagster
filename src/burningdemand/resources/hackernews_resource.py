"""Hacker News resource: collects stories for a given date."""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx
from dagster import ConfigurableResource
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.assets.raw.model import RawItem
from burningdemand.utils.requests import batch_requests
from burningdemand.utils.config import config
from burningdemand.utils.url import iso_date_to_utc_bounds


RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=100,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_hn.db",
    use_file_lock=True,
)


class HackerNewsResource(ConfigurableResource):
    """Collects Hacker News stories for the given date."""

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

    async def collect(self, date: str) -> Tuple[List[RawItem], Dict]:
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

        response_pairs = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )

        pages = [
            resp.json().get("hits", [])
            for _, resp in response_pairs
            if resp is not None
        ]
        items = []
        for hits in pages:
            for it in hits:
                title = it.get("title") or ""
                body = it.get("story_text") or ""
                if config.matches_keywords(f"{title} {body}", "hackernews"):
                    created_i = int(it.get("created_at_i") or 0)
                    items.append(
                        RawItem(
                            url=it.get("url")
                            or f"https://news.ycombinator.com/item?id={it.get('objectID')}",
                            title=title,
                            body=body[
                                : config.issues.labeling.max_body_length_for_snippet
                            ],
                            created_at=(
                                datetime.fromtimestamp(
                                    created_i, tz=timezone.utc
                                ).isoformat()
                                if created_i
                                else ""
                            ),
                            source_post_id=str(it.get("objectID") or ""),
                            comments_list=[],
                            comments_count=it.get("num_comments", 0) or 0,
                            vote_count=it.get("points", 0) or 0,
                            post_type="story",
                            reactions_count=0,
                            org_name="",
                            product_name="",
                        )
                    )

        ok_count = sum(1 for _, r in response_pairs if r is not None)
        self._context.log.info(
            f"HackerNews: {ok_count}/{len(response_pairs)} requests ok, {len(items)} items"
        )
        return items, {"requests": len(response_pairs), "ok": ok_count}
