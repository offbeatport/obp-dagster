"""GitHub issues collector."""

import asyncio
from typing import Dict, List, Tuple

import httpx
from dagster import ConfigurableResource, EnvVar
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.utils.batch_requests import batch_requests
from burningdemand.utils.config import config
from burningdemand.utils.date_ranges import split_day_into_ranges


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
        self._client: httpx.AsyncClient | None = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={"User-Agent": "BurningDemand/0.1"},
        )
        self._cfg = config.collectors.github

    def teardown_after_execution(self, context) -> None:
        if hasattr(self, "_client") and self._client is not None:
            try:
                asyncio.run(self._client.aclose())
            except Exception:
                pass
            self._client = None

    async def collect(
        self, date: str, post_type: str = "issue"
    ) -> Tuple[List[Dict], Dict]:
        """Collect GitHub issues or discussions for the given date.
        post_type: "issue" | "discussion" (discussion not yet implemented).
        """
        if self._client is None:
            raise RuntimeError("GitHubCollector client is not initialized")
        if post_type == "discussion":
            return await self._collect_discussions(date)

        specs = self._generate_specs(date, post_type="issue")
        responses = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )

        items = []

        for resp in responses:
            for it in resp.json().get("items", []):
                url = it.get("html_url")
                items.append(
                    {
                        "url": url,
                        "title": it.get("title"),
                        "body": it.get("body"),
                        "created_at": it.get("created_at"),
                        "source_post_id": str(it.get("id")),
                        "comment_count": it.get("comments", 0),
                        "vote_count": 0,
                        "post_type": "issue",
                        "reaction_count": it.get("reactions", {}).get("total_count", 0)
                        or 0,
                        "org_name": url.split("/")[3],
                        "product_name": url.split("/")[4],
                    }
                )

        log = getattr(getattr(self, "_context", None), "log", None)
        if log is not None:
            log.info(f"GitHub issues: {len(responses)} requests, {len(items)} items")
        return items, {"requests": len(responses)}

    async def _collect_discussions(self, date: str) -> Tuple[List[Dict], Dict]:
        """Collect GitHub discussions. Stub: not yet implemented."""
        log = getattr(getattr(self, "_context", None), "log", None)
        if log is not None:
            log.warning("GitHub discussions collector not yet implemented")
        return [], {"requests": 0, "note": "discussions not implemented"}

    def _generate_specs(self, date: str, post_type: str = "issue") -> List[dict]:
        queries = []

        ranges = split_day_into_ranges(date, self._cfg.queries_per_day)
        # Only issues supported in search API today
        q_prefix = "is:issue"
        for start_z, end_z in ranges:
            queries.append(
                f"{q_prefix} created:{start_z}..{end_z} comments:>{self._cfg.min_comments} reactions:>{self._cfg.min_reactions}"
            )

        specs = [
            {
                "method": "GET",
                "url": "https://api.github.com/search/issues",
                "params": {
                    "q": q,
                    "per_page": self._cfg.per_page,
                    "page": p,
                },
                "headers": {"Authorization": f"token {self.github_token}"},
            }
            for q in queries
            for p in range(1, self._cfg.max_pages + 1)
        ]

        return specs
