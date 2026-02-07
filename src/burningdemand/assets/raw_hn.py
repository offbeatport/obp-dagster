"""Raw Hacker News stories asset."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.hackernews_resource import HackerNewsResource

from burningdemand.utils.raw_utils import materialize_raw


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Hacker News stories per day. Writes into bronze.raw_items (source=hn, post_type=story).",
)
async def raw_hn(
    context: AssetExecutionContext,
    db: DuckDBResource,
    hackernews: HackerNewsResource,
) -> MaterializeResult:
    items, meta = await hackernews.collect(context.partition_key)
    return await materialize_raw(db, items, meta, "hn", context.partition_key)
