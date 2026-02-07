"""Day-partitioned raw assets (GitHub, Reddit, StackOverflow, Hacker News). All write into bronze.raw_items."""

from typing import Any, Dict, List

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.schema.raw_items import CollectedItems, RawItem
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.utils.config import config


UPSERT_COLUMNS = [
    "source",
    "source_post_id",
    "post_type",
    "url_hash",
    "collection_date",
    "url",
    "title",
    "body",
    "created_at",
    "comment_count",
    "vote_count",
    "org_name",
    "product_name",
    "reaction_count",
]


async def materialize_raw(
    db: DuckDBResource,
    items: List[RawItem],
    meta: Dict[str, Any],
    source: str,
    date: str,
) -> MaterializeResult:
    """Upsert collected items into bronze.raw_items. Returns empty result if no items."""
    if not items:
        return MaterializeResult(
            metadata={
                "source": source,
                "date": date,
                "collected": 0,
                "insert_attempted": 0,
                "collector": meta,
            }
        )
    collected = CollectedItems(items, meta)
    df = collected.to_df(source, date)
    inserted_attempt = db.upsert_df("bronze", "raw_items", df, UPSERT_COLUMNS)
    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": len(df),
            "insert_attempted": inserted_attempt,
            "collector": meta,
        }
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub issues per day. Writes into bronze.raw_items (source=gh_issues, post_type=issue).",
)
async def raw_gh_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:
    date = context.partition_key
    items, meta = await github.fetch(
        "search/issues",
        date,
        f"is:issue comments:>{config.collectors.github.min_comments} reactions:>{config.collectors.github.min_reactions}",
    )
    return await materialize_raw(db, items, meta, "gh_issues", date)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub discussions per day. Writes into bronze.raw_items (source=gh_discussions, post_type=discussion).",
)
async def raw_gh_discussions(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:
    date = context.partition_key
    items, meta = await github.fetch("search/issues", date, "is:discussion")
    return await materialize_raw(db, items, meta, "gh_discussions", date)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Reddit posts per day. Writes into bronze.raw_items (source=rd, post_type=post).",
)
async def raw_rd(
    context: AssetExecutionContext,
    db: DuckDBResource,
    reddit,
) -> MaterializeResult:
    items, meta = await reddit.collect(context.partition_key)
    return await materialize_raw(db, items, meta, "rd", context.partition_key)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Stack Overflow questions per day. Writes into bronze.raw_items (source=so, post_type=question).",
)
async def raw_so(
    context: AssetExecutionContext,
    db: DuckDBResource,
    stackoverflow,
) -> MaterializeResult:
    items, meta = await stackoverflow.collect(context.partition_key)
    return await materialize_raw(db, items, meta, "so", context.partition_key)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Hacker News stories per day. Writes into bronze.raw_items (source=hn, post_type=story).",
)
async def raw_hn(
    context: AssetExecutionContext,
    db: DuckDBResource,
    hackernews,
) -> MaterializeResult:
    items, meta = await hackernews.collect(context.partition_key)
    return await materialize_raw(db, items, meta, "hn", context.partition_key)
