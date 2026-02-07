"""Composite collectors resource that delegates to per-source collectors."""

from typing import Dict, List, Tuple

from dagster import ConfigurableResource

from .github_collector import GitHubCollector
from .stackoverflow_collector import StackOverflowCollector
from .reddit_collector import RedditCollector
from .hackernews_collector import HackerNewsCollector

RAW_ASSET_KEYS = ("gh_issues", "gh_discussions", "rd", "so", "hn")


class CollectorsResource(ConfigurableResource):
    """Resource that holds one collector per source and delegates by asset key."""

    github_collector: GitHubCollector
    stackoverflow_collector: StackOverflowCollector
    reddit_collector: RedditCollector
    hackernews_collector: HackerNewsCollector

    def setup_for_execution(self, context) -> None:
        self._context = context
        self.github_collector.setup_for_execution(context)
        self.stackoverflow_collector.setup_for_execution(context)
        self.reddit_collector.setup_for_execution(context)
        self.hackernews_collector.setup_for_execution(context)

    def teardown_after_execution(self, context) -> None:
        self.github_collector.teardown_after_execution(context)
        self.stackoverflow_collector.teardown_after_execution(context)
        self.reddit_collector.teardown_after_execution(context)
        self.hackernews_collector.teardown_after_execution(context)

    async def collect_by_asset(
        self,
        asset_key: str,
        date: str,
    ) -> Tuple[List[Dict], Dict]:
        """Collect items for a single raw_* asset partition (day only).
        asset_key: one of gh_issues, gh_discussions, rd, so, hn.
        """
        if asset_key == "gh_issues":
            return await self.github_collector.collect(date, post_type="issue")
        if asset_key == "gh_discussions":
            return await self.github_collector.collect(date, post_type="discussion")
        if asset_key == "rd":
            return await self.reddit_collector.collect(date)
        if asset_key == "so":
            return await self.stackoverflow_collector.collect(date)
        if asset_key == "hn":
            return await self.hackernews_collector.collect(date)
        return [], {"requests": 0, "note": f"unknown asset_key={asset_key}"}
