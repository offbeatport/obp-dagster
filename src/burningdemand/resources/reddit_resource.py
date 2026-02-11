"""Reddit resource: collects posts for a given date."""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx
from dagster import ConfigurableResource
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.model.raw_items import RawItem
from burningdemand.utils.requests import batch_requests
from burningdemand.utils.config import config
from burningdemand.utils.url import iso_date_to_utc_bounds

RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=60,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_rd.db",
    use_file_lock=True,
)


class RedditResource(ConfigurableResource):
    """Collects Reddit posts for the given date."""

    reddit_client_id: Optional[str] = None
    reddit_client_secret: Optional[str] = None

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
        """Collect Reddit posts for the given date."""
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        client_id = self.reddit_client_id
        client_secret = self.reddit_client_secret
        user_agent = "BurningDemand/0.1"
        subreddits = config.get_reddit_subreddits()

        token = None
        req_count = 0

        if client_id and client_secret:
            token_specs = [
                {
                    "method": "POST",
                    "url": "https://www.reddit.com/api/v1/access_token",
                    "auth": (client_id, client_secret),
                    "data": {"grant_type": "client_credentials"},
                    "headers": {"User-Agent": user_agent},
                }
            ]
            token_pairs = await batch_requests(
                self._client, self._context, token_specs, limiter=RATE_LIMITER
            )
            req_count += len(token_pairs)
            for _, r in token_pairs:
                if r is not None:
                    token = r.json().get("access_token")
                    break

        base = "https://oauth.reddit.com" if token else "https://api.reddit.com"
        headers = {"User-Agent": user_agent}
        if token:
            headers["Authorization"] = f"bearer {token}"

        self._context.log.info(
            f"Reddit: {len(subreddits)} subreddits, {'OAuth' if token else 'public'}"
        )

        sub_specs = [
            {
                "method": "GET",
                "url": f"{base}/r/{sub}/new",
                "params": {"limit": 100},
                "headers": headers,
            }
            for sub in subreddits
        ]

        sub_pairs = await batch_requests(
            self._client, self._context, sub_specs, limiter=RATE_LIMITER
        )
        req_count += len(sub_pairs)

        results = [
            r.json().get("data", {}).get("children", [])
            for _, r in sub_pairs
            if r is not None
        ]
        items = []
        for children in results:
            for ch in children:
                d = ch.get("data") or {}
                created = int(d.get("created_utc") or 0)
                if from_ts <= created < to_ts:
                    title = d.get("title") or ""
                    body = d.get("selftext") or ""
                    if config.matches_keywords(f"{title} {body}", "reddit"):
                        items.append(
                            RawItem(
                                url=f"https://reddit.com{d.get('permalink','')}",
                                title=title,
                                body=body[
                                    : config.issues.labeling.max_body_length_for_snippet
                                ],
                                created_at=datetime.fromtimestamp(
                                    created, tz=timezone.utc
                                ).isoformat(),
                                source_post_id=str(d.get("id") or ""),
                                comments_list=[],
                                comments_count=d.get("num_comments", 0) or 0,
                                vote_count=d.get("score", 0) or 0,
                                post_type="post",
                                reactions_count=0,
                                org_name="",
                                product_name="",
                            )
                        )

        self._context.log.info(f"Reddit: {req_count} requests, {len(items)} items")
        return items, {
            "requests": req_count,
            "subs": subreddits,
            "used_oauth": bool(token),
        }
