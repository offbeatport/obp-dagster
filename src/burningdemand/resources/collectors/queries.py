"""Query configuration for collectors: delegates to config for keywords, tags, subreddits."""

from typing import List, Optional

from burningdemand.utils.config import config


def get_query_keywords(source: str) -> List[str]:
    """Get all keywords for a source (core + source-specific)."""
    return config.get_source_keywords(source)


def get_query_tags(source: str) -> List[str]:
    """Get tags for a source."""
    return config.get_source_tags(source)


def get_query_subreddits() -> List[str]:
    """Get Reddit subreddits to monitor."""
    return config.get_reddit_subreddits()


def matches_query_keywords(text: str, source: str) -> bool:
    """Check if text matches any keywords for the given source."""
    return config.matches_keywords(text, source)


def get_body_max_length() -> int:
    """Get maximum body length for collected items."""
    return 10 * 1000  # 10KB


def get_max_queries(source: str) -> Optional[int]:
    """Get maximum number of queries to execute for a source (None = no limit)."""
    return None
