"""Composite collectors resource that delegates to per-source collectors."""

from typing import Any, Dict, Tuple

from dagster import ConfigurableResource

from burningdemand.resources.collectors.types import CollectedItems, RawItem

from .github_collector import GitHubCollector
from .hackernews_collector import HackerNewsCollector
from .reddit_collector import RedditCollector
from .stackoverflow_collector import StackOverflowCollector

# source -> (collector attribute name, kwargs for .collect(date, **kwargs))
_SOURCE_DISPATCH: Dict[str, Tuple[str, Dict[str, Any]]] = {
    "gh_issues": ("github_collector", {"post_type": "issue"}),
    "gh_discussions": ("github_collector", {"post_type": "discussion"}),
    "rd": ("reddit_collector", {}),
    "so": ("stackoverflow_collector", {}),
    "hn": ("hackernews_collector", {}),
}

_COLLECTORS = ["github_collector", "stackoverflow_collector", "reddit_collector", "hackernews_collector"]


class CollectorsResource(ConfigurableResource):
    """Resource that holds one collector per source and delegates by asset key."""

    github_collector: GitHubCollector
    stackoverflow_collector: StackOverflowCollector
    reddit_collector: RedditCollector
    hackernews_collector: HackerNewsCollector

    def setup_for_execution(self, context) -> None:
        self._context = context
        for name in _COLLECTORS:
            getattr(self, name).setup_for_execution(context)

    def teardown_after_execution(self, context) -> None:
        for name in _COLLECTORS:
            getattr(self, name).teardown_after_execution(context)

    async def collect(self, source: str, date: str) -> CollectedItems:
        """Collect for a single raw_* asset partition (day only).

        Returns a CollectedItems with .items (list of RawItem) and .meta (dict).
        Use .to_df(source, date) to build the DataFrame for upsert into bronze.raw_items.

        source: one of gh_issues, gh_discussions, rd, so, hn.
        """
        entry = _SOURCE_DISPATCH.get(source)
        if entry is None:
            return CollectedItems([], {"requests": 0, "note": f"unknown source={source}"})
        collector_name, kwargs = entry
        collector = getattr(self, collector_name)
        items, meta = await collector.collect(date, **kwargs)
        return CollectedItems(items, meta)
