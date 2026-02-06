"""GitHub issues collector."""

import asyncio
from typing import Dict, List, Tuple

import httpx
from dagster import ConfigurableResource, EnvVar
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import (
    create_sqlite_limiter,
)

from burningdemand.utils.batch_requests import batch_requests
from .queries import (
    get_body_max_length,
    get_max_queries,
    get_query_keywords,
    matches_query_keywords,
)


# 30 requests per minute (shared across collect() calls)
RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=30,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_gh.db",
    use_file_lock=True,
)


class GitHubCollector(ConfigurableResource):
    """Collector for GitHub issues."""

    github_token: str = EnvVar("GITHUB_TOKEN")

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
        """Collect GitHub issues for the given date."""
        headers = {"Authorization": f"token {self.github_token}"}
        keywords = get_query_keywords("github")

        queries = []
        for i in range(0, len(keywords), 6):
            chunk = keywords[i : i + 6]
            keyword_query = " OR ".join([f'"{kw}"' for kw in chunk])
            queries.append(f"is:issue created:{date} ({keyword_query})")

        max_queries = get_max_queries("github")
        if max_queries is not None:
            queries = queries[:max_queries]

        specs = [
            {
                "method": "GET",
                "url": "https://api.github.com/search/issues",
                "params": {"q": q, "per_page": 100, "page": p},
                "headers": headers,
            }
            for q in queries
            for p in range(1, 11)
        ]

        responses = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )
        seen = set()
        items = []
        for resp in responses:
            for it in resp.json().get("items", []):
                url = it.get("html_url")
                if not url or url in seen:
                    continue
                title = it.get("title") or ""
                body = it.get("body") or ""
                if matches_query_keywords(f"{title} {body}", "github"):
                    seen.add(url)
                    items.append(
                        {
                            "url": url,
                            "title": title,
                            "body": body[: get_body_max_length()],
                            "created_at": it.get("created_at") or "",
                            "comment_count": it.get("comments", 0) or 0,
                            "vote_count": it.get("reactions.total_count", 0) or 0,
                            "org_name": url.split("/")[3],
                            "product_name": url.split("/")[4],
                        }
                    )

        self._context.log.info(f"GitHub: {len(responses)} requests, {len(items)} items")
        return items, {"requests": len(responses), "queries": len(queries)}
