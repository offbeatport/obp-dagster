"""Raw Stack Overflow questions asset."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.stackoverflow_resource import StackOverflowResource

from .materialize import materialize_raw


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Stack Overflow questions per day. Writes into bronze.raw_items (source=so, post_type=question).",
)
async def raw_so(
    context: AssetExecutionContext,
    db: DuckDBResource,
    stackoverflow: StackOverflowResource,
) -> MaterializeResult:
    items, meta = await stackoverflow.collect(context.partition_key)
    return await materialize_raw(db, items, meta, "so", context.partition_key)
