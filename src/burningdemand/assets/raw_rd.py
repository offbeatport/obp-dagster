"""Raw Reddit posts asset."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.reddit_resource import RedditResource

from burningdemand.utils.raw_utils import materialize_raw


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Reddit posts per day. Writes into bronze.raw_items (source=rd, post_type=post).",
)
async def raw_rd(
    context: AssetExecutionContext,
    db: DuckDBResource,
    reddit: RedditResource,
) -> MaterializeResult:
    items, meta = await reddit.collect(context.partition_key)
    return await materialize_raw(db, items, meta, "rd", context.partition_key)
