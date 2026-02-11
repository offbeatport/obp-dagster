"""StackOverflow resource: collects questions for a given date."""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx
from dagster import ConfigurableResource
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.assets.raw.model import RawItem
from burningdemand.utils.requests import batch_requests
from burningdemand.utils.config import config
from burningdemand.utils.url import iso_date_to_utc_bounds


RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=60,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_so.db",
    use_file_lock=True,
)


class StackOverflowResource(ConfigurableResource):
    """Collects StackOverflow questions for the given date."""

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

    async def collect(self, date: str) -> Tuple[List[RawItem], Dict]:
        """Collect StackOverflow questions for the given date."""
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        tags = config.build_stackoverflow_tags()
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

        response_pairs = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )

        pages = [
            resp.json().get("items", [])
            for _, resp in response_pairs
            if resp is not None
        ]
        items = []
        for page_items in pages:
            for it in page_items:
                title = it.get("title") or ""
                body = it.get("body_markdown") or it.get("body") or ""
                if config.matches_keywords(f"{title} {body}", "stackoverflow"):
                    created = it.get("creation_date")
                    items.append(
                        RawItem(
                            url=it.get("link") or "",
                            title=title,
                            body=body[
                                : config.issues.labeling.max_body_length_for_snippet
                            ],
                            created_at=(
                                datetime.fromtimestamp(
                                    int(created), tz=timezone.utc
                                ).isoformat()
                                if created
                                else ""
                            ),
                            source_post_id=str(it.get("question_id") or ""),
                            comments_list=[],
                            comments_count=it.get("answer_count", 0) or 0,
                            vote_count=it.get("score", 0) or 0,
                            post_type="question",
                            reactions_count=0,
                            org_name="",
                            product_name="",
                        )
                    )

        ok_count = sum(1 for _, r in response_pairs if r is not None)
        self._context.log.info(
            f"StackOverflow: {ok_count}/{len(response_pairs)} requests ok, {len(items)} items"
        )
        return items, {
            "requests": len(response_pairs),
            "ok": ok_count,
            "used_key": bool(key),
        }
