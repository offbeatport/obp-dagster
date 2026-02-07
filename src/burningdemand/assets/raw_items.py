"""Day-partitioned raw assets per source. All write into bronze.raw_items."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.collectors.collectors_resource import CollectorsResource
from burningdemand.resources.duckdb_resource import DuckDBResource


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


async def _materialize_raw(
    db: DuckDBResource,
    collector: CollectorsResource,
    source: str,
    date: str,
) -> MaterializeResult:
    """Collect for (source, date) and upsert into bronze.raw_items.
    source: asset key like gh_issues, gh_discussions, rd, so, hn (used as bronze.raw_items.source).
    """
    collected = await collector.collect(source, date)

    if not collected.items:
        return MaterializeResult(
            metadata={
                "source": source,
                "date": date,
                "collected": 0,
                "insert_attempted": 0,
                "collector": collected.meta,
            }
        )

    df = collected.to_df(source, date)

    inserted_attempt = db.upsert_df(
        "bronze",
        "raw_items",
        df,
        UPSERT_COLUMNS,
    )

    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": int(len(df)),
            "insert_attempted": int(inserted_attempt),
            "collector": collected.meta,
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
    collector: CollectorsResource,
) -> MaterializeResult:
    return await _materialize_raw(db, collector, "gh_issues", context.partition_key)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub discussions per day. Writes into bronze.raw_items (source=gh_discussions, post_type=discussion). Stub until implemented.",
)
async def raw_gh_discussions(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    return await _materialize_raw(db, collector, "gh_discussions", context.partition_key)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Reddit posts per day. Writes into bronze.raw_items (source=rd, post_type=post).",
)
async def raw_rd(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    return await _materialize_raw(db, collector, "rd", context.partition_key)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Stack Overflow questions per day. Writes into bronze.raw_items (source=so, post_type=question).",
)
async def raw_so(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    return await _materialize_raw(db, collector, "so", context.partition_key)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Hacker News stories per day. Writes into bronze.raw_items (source=hn, post_type=story).",
)
async def raw_hn(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    return await _materialize_raw(db, collector, "hn", context.partition_key)
