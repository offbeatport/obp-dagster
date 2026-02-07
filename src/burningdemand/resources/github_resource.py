"""GitHub resource: fetches issues or discussions by path, day, and query."""

import asyncio
from typing import Dict, List, Tuple

import httpx

from burningdemand.schema.raw_items import RawItem
from dagster import ConfigurableResource, EnvVar
from pyrate_limiter import Duration
from pyrate_limiter.limiter_factory import create_sqlite_limiter

from burningdemand.utils.config import config
from burningdemand.utils.date_ranges import split_day_into_ranges
from burningdemand.utils.requests import batch_requests

RATE_LIMITER = create_sqlite_limiter(
    rate_per_duration=30,
    duration=Duration.MINUTE,
    buffer_ms=5000,
    db_path="data/rate_limiter_gh.db",
    use_file_lock=True,
)


class GitHubResource(ConfigurableResource):
    """Fetches GitHub API by path, day, and query. Builds request specs internally."""

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

    async def fetch(
        self, path: str, day: str, query: str
    ) -> Tuple[List[RawItem], Dict]:
        """Build specs from path, day, query; execute and parse into RawItem list.
        post_type is inferred from query ("is:discussion" -> discussion, else issue).
        """
        if self._client is None:
            raise RuntimeError("GitHubResource client not initialized")

        cfg = config.collectors.github
        ranges = split_day_into_ranges(day, cfg.queries_per_day)
        full_queries = [
            f"created:{start_z}..{end_z} {query}" for start_z, end_z in ranges
        ]
        specs = [
            {
                "method": "GET",
                "url": f"https://api.github.com/{path.lstrip('/')}",
                "params": {"q": q, "per_page": cfg.per_page, "page": p},
                "headers": {"Authorization": f"token {self.github_token}"},
            }
            for q in full_queries
            for p in range(1, cfg.max_pages + 1)
        ]
        if not specs:
            return [], {"requests": 0, "ok": 0}

        post_type = "discussion" if "is:discussion" in query else "issue"

        response_pairs = await batch_requests(
            self._client,
            self._context,
            specs,
            limiter=RATE_LIMITER,
        )

        max_body = config.labeling.max_body_length_for_snippet
        items: List[RawItem] = []
        for _, resp in response_pairs:
            if resp is None:
                continue
            for it in resp.json().get("items", []):
                url = it.get("html_url") or ""
                parts = url.rstrip("/").split("/")
                org_name = parts[3] if len(parts) > 3 else ""
                product_name = parts[4] if len(parts) > 4 else ""
                body = (it.get("body") or "")[:max_body]
                items.append(
                    RawItem(
                        url=url,
                        title=it.get("title") or "",
                        body=body,
                        created_at=it.get("created_at") or "",
                        source_post_id=str(it.get("id")),
                        comment_count=it.get("comments", 0),
                        vote_count=0,
                        post_type=post_type,
                        reaction_count=it.get("reactions", {}).get("total_count", 0)
                        or 0,
                        org_name=org_name,
                        product_name=product_name,
                    )
                )

        ok_count = sum(1 for _, r in response_pairs if r is not None)
        log = getattr(getattr(self, "_context", None), "log", None)
        if log is not None:
            log.info(
                f"GitHub {post_type}: {ok_count}/{len(response_pairs)} requests ok, {len(items)} items"
            )
        return items, {"requests": len(response_pairs), "ok": ok_count}
