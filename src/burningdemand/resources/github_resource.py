"""GitHub resource: REST and GraphQL fetch for issues/discussions (githubkit)."""

import asyncio
from typing import Any, Dict, List, Optional, Tuple

from githubkit import GitHub
from githubkit.exception import GraphQLFailed

from dagster import ConfigurableResource, EnvVar

from burningdemand.utils.date_ranges import split_day_into_ranges


class GitHubResource(ConfigurableResource):
    github_token: str = EnvVar("GITHUB_TOKEN")

    def setup_for_execution(self, context) -> None:
        self._context = context
        self._gh = GitHub(
            self.github_token, user_agent="BurningDemand/0.1", timeout=30.0
        )

    def teardown_after_execution(self, context) -> None:
        self._gh = None

    @property
    def log(self):
        # Dagster ensures context.log exists during execution
        return self._context.log

    async def _search_range(
        self,
        range_idx: int,
        total_ranges: int,
        start_z: str,
        end_z: str,
        suffix: str,
        gql_query: str,
        per_page: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Fetch all pages for a single time range."""
        query_str = f"created:{start_z}..{end_z} {suffix}".strip()
        cursor: Optional[str] = None
        page_in_range = 0
        items: List[Dict[str, Any]] = []
        n_requests = 0

        self.log.info(
            "GitHub GraphQL: range %s/%s %s..%s",
            range_idx,
            total_ranges,
            start_z,
            end_z,
        )

        while True:
            try:
                root = await self._gh.graphql.arequest(
                    gql_query,
                    {"queryStr": query_str, "first": per_page, "after": cursor},
                )
            except GraphQLFailed as e:
                self.log.error(
                    "GitHub GraphQL failed (range %s/%s %s..%s, page %s): %s",
                    range_idx,
                    total_ranges,
                    start_z,
                    end_z,
                    page_in_range + 1,
                    e.response.errors,
                )
                raise

            n_requests += 1
            page_in_range += 1
            search = root.get("search") or {}
            # Some queries (issues vs discussions) may or may not have discussionCount
            if "discussionCount" in search:
                self.log.info("discussionCount: %s", search.get("discussionCount"))
            nodes = [n for n in (search.get("nodes") or []) if n]
            items.extend(nodes)
            rate = root.get("rateLimit") or {}
            self.log.info(
                "GitHub GraphQL: request %s — range %s/%s page %s: +%s items (total %s) - rateLimit (%s)",
                n_requests,
                range_idx,
                total_ranges,
                page_in_range,
                len(nodes),
                len(items),
                rate,
            )
            page_info = search.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return items, n_requests

    async def search(
        self,
        day: str,
        node_fragment: str,
        type: str = "ISSUE",
        query_suffix: str = "",
        hour_splits: int = 2,
        per_page: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Dict]:
        """GraphQL search: return raw nodes with the provided fragment."""

        if not getattr(self, "_gh", None):
            raise RuntimeError("GitHubResource client not initialized")

        suffix = (query_suffix if isinstance(query_suffix, str) else "").strip()
        gql_query = f"""
        query GitHubSearch($queryStr: String!, $first: Int!, $after: String) {{          
          rateLimit {{ remaining used resetAt nodeCount limit cost }}
          search(query: $queryStr, type: {type}, first: $first, after: $after) {{            
            pageInfo {{ hasNextPage endCursor }}
            nodes {{
              __typename
              {node_fragment}
            }}
          }}
        }}
        """
        ranges = split_day_into_ranges(day, hour_splits)
        total_ranges = len(ranges)

        # Run each time-range query in parallel
        tasks = [
            self._search_range(
                range_idx=idx,
                total_ranges=total_ranges,
                start_z=start_z,
                end_z=end_z,
                suffix=suffix,
                gql_query=gql_query,
                per_page=per_page,
            )
            for idx, (start_z, end_z) in enumerate(ranges, start=1)
        ]
        results = await asyncio.gather(*tasks)

        items: List[Dict[str, Any]] = []
        n_requests = 0
        for range_items, range_requests in results:
            items.extend(range_items)
            n_requests += range_requests

        self.log.info("GitHub GraphQL: %s items", len(items))
        return items, {"requests": n_requests, "ok": n_requests}
