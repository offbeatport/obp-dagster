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

    def _log(self):
        return getattr(getattr(self, "_context", None), "log", None)

    async def fetch(
        self, path: str, day: str, query: str
    ) -> Tuple[List[Dict[str, Any]], Dict]:
        """REST search/issues: return raw API items."""

        if not getattr(self, "_gh", None):
            raise RuntimeError("GitHubResource client not initialized")
        cfg = self._cfg
        ranges = split_day_into_ranges(day, cfg.queries_per_day)
        queries = [f"created:{s}..{e} {query}" for s, e in ranges]
        if not queries:
            return [], {"requests": 0, "ok": 0}
        items: List[Dict[str, Any]] = []
        ok = 0
        for q in queries:
            for p in range(1, cfg.max_pages + 1):
                resp = await self._gh.arequest(
                    "GET",
                    f"/{path.lstrip('/')}",
                    params={"q": q, "per_page": cfg.per_page, "page": p},
                )
                ok += 1
                items.extend((resp.parsed_data or {}).get("items", []) or [])

        self._log().info(f"GitHub REST: {len(items)} items")
        return items, {"requests": len(queries) * cfg.max_pages, "ok": ok}

    async def search(
        self,
        day: str,
        node_fragment: str,
        query_suffix: str = "",
        hour_splits: int = 24,
    ) -> Tuple[List[Dict[str, Any]], Dict]:
        """GraphQL search: return raw nodes with the provided fragment."""

        if not getattr(self, "_gh", None):
            raise RuntimeError("GitHubResource client not initialized")

        suffix = query_suffix.strip()
        gql_query = f"""
        query GitHubSearch($queryStr: String!, $first: Int!, $after: String) {{
          rateLimit {{ limit remaining resetAt }}
          search(query: $queryStr, type: ISSUE, first: $first, after: $after) {{
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
        log = self._log()

        for range_idx, (start_z, end_z) in enumerate(ranges, start=1):
            query_str = f"created:{start_z}..{end_z} {suffix}".strip()
            cursor = None
            page_in_range = 0
            if log:
                log.info(
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
                if not search:
                    break
                nodes = [n for n in (search.get("nodes") or []) if n]
                items.extend(nodes)
                rate = root.get("rateLimit") or {}
                remaining = rate.get("remaining")
                if log:
                    msg = (
                        "GitHub GraphQL: request %s — range %s/%s page %s: +%s items (total %s)"
                        % (
                            n_requests,
                            range_idx,
                            total_ranges,
                            page_in_range,
                            len(nodes),
                            len(items),
                        )
                    )
                    if remaining is not None:
                        msg += " — rate limit remaining: %s" % remaining
                    log.info(msg)
                pi = search.get("pageInfo") or {}
                if not pi.get("hasNextPage"):
                    break
                cursor = pi.get("endCursor")

        if log:
            log.info("GitHub GraphQL: %s items", len(items))
        return items, {"requests": n_requests, "ok": n_requests}
