"""GitHub resource: REST and GraphQL fetch for issues/discussions (githubkit)."""

from typing import Any, Dict, List, Optional, Tuple

from githubkit import GitHub

from dagster import ConfigurableResource, EnvVar

from burningdemand.utils.config import config
from burningdemand.utils.date_ranges import split_day_into_ranges


class GitHubResource(ConfigurableResource):
    github_token: str = EnvVar("GITHUB_TOKEN")

    def setup_for_execution(self, context) -> None:
        self._context = context
        self._gh = GitHub(
            self.github_token, user_agent="BurningDemand/0.1", timeout=30.0
        )
        self._cfg = config.resources.github

    def teardown_after_execution(self, context) -> None:
        self._gh = None

    @property
    def log(self):
        # Dagster ensures context.log exists during execution
        return self._context.log

    async def search(
        self,
        day: str,
        node_fragment: str,
        type: str = "ISSUE",
        query_suffix: str = "",
        hour_splits: int = 2,
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
        items: List[Dict[str, Any]] = []
        n_requests = 0
        total_ranges = len(ranges)

        for range_idx, (start_z, end_z) in enumerate(ranges, start=1):
            query_str = f"created:{start_z}..{end_z} {suffix}".strip()
            cursor = None
            page_in_range = 0
            self.log.info(
                "GitHub GraphQL: range %s/%s %s..%s",
                range_idx,
                total_ranges,
                start_z,
                end_z,
            )
            while True:
                root = await self._gh.graphql.arequest(
                    gql_query, {"queryStr": query_str, "first": 100, "after": cursor}
                )
                n_requests += 1
                page_in_range += 1
                search = root.get("search")
                s = search.get("discussionCount")
                self.log.info("discussionCount: %s", s)
                if not search:
                    break
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
                pi = search.get("pageInfo") or {}
                if not pi.get("hasNextPage"):
                    break
                cursor = pi.get("endCursor")

        self.log.info("GitHub GraphQL: %s items", len(items))
        return items, {"requests": n_requests, "ok": n_requests}
