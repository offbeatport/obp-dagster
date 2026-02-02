"""StackOverflow questions collector."""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx
from dagster import ConfigurableResource
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.utils.batch_requests import batch_requests
from burningdemand.utils.url import iso_date_to_utc_bounds
from .queries import (
    get_body_max_length,
    get_query_tags,
    matches_query_keywords,
)


RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=60,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_so.db",
    use_file_lock=True,
)


class StackOverflowCollector(ConfigurableResource):
    """Collector for StackOverflow questions."""

    stackexchange_key: Optional[str] = None

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
        """Collect StackOverflow questions for the given date."""
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        tags = get_query_tags("stackoverflow")
        key = self.stackexchange_key

        self._context.log.info("StackOverflow: 10 pages")
        specs = []
        for page in range(1, 11):
            params = {
                "fromdate": from_ts,
                "todate": to_ts,
                "site": "stackoverflow",
                "pagesize": 100,
                "page": page,
                "filter": "withbody",
            }
            if tags:
                params["tagged"] = ";".join(tags)
            if key:
                params["key"] = key
            specs.append(
                {
                    "method": "GET",
                    "url": "https://api.stackexchange.com/2.3/questions",
                    "params": params,
                }
            )

        responses = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )

        pages = [resp.json().get("items", []) for resp in responses]
        items = []
        for page_items in pages:
            for it in page_items:
                title = it.get("title") or ""
                body = it.get("body_markdown") or it.get("body") or ""
                if matches_query_keywords(f"{title} {body}", "stackoverflow"):
                    created = it.get("creation_date")
                    items.append(
                        {
                            "url": it.get("link") or "",
                            "title": title,
                            "body": body[: get_body_max_length()],
                            "created_at": (
                                datetime.fromtimestamp(
                                    int(created), tz=timezone.utc
                                ).isoformat()
                                if created
                                else ""
                            ),
                            "comment_count": it.get("answer_count", 0) or 0,
                            "vote_count": it.get("score", 0) or 0,
                        }
                    )

        self._context.log.info(
            f"StackOverflow: {len(responses)} requests, {len(items)} items"
        )
        return items, {"requests": len(responses), "used_key": bool(key)}
