"""Composite collectors resource that delegates to per-source collectors."""

from typing import Dict, List, Tuple

from dagster import ConfigurableResource

from .github_collector import GitHubCollector
from .stackoverflow_collector import StackOverflowCollector
from .reddit_collector import RedditCollector
from .hackernews_collector import HackerNewsCollector


class CollectorsResource(ConfigurableResource):
    """Resource that holds one collector per source and delegates collect(source, date)."""

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

    async def collect(
        self,
        source: str,
        date: str,
    ) -> Tuple[List[Dict], Dict]:
        """Collect items for a single (source, date) partition."""
        collectors = {
            "github": self.github_collector,
            "stackoverflow": self.stackoverflow_collector,
            "reddit": self.reddit_collector,
            "hackernews": self.hackernews_collector,
        }
        collector = collectors.get(source)
        if not collector:
            return [], {"requests": 0, "note": f"unknown source={source}"}
        return await collector.collect(date)
