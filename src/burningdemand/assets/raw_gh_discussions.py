"""Raw GitHub discussions asset."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.utils.config import config

from burningdemand.utils.raw_utils import gh_to_raw_item, materialize_raw


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
    node_fragment = (
        "... on Discussion { id number url title body createdAt "
        "repository { nameWithOwner } comments { totalCount } "
        "reactions { totalCount } }"
    )
    query_suffix = (
        f"is:discussion comments:>{config.resources.github.min_comments} "
        f"reactions:>{config.resources.github.min_reactions}"
    )
    raw_items, meta = await github.search(
        date, node_fragment, query_suffix=query_suffix
    )
    max_body = config.labeling.max_body_length_for_snippet
    items = [
        item for item in (gh_to_raw_item(d, "discussion", max_body) for d in raw_items)
    ]
    return await materialize_raw(db, items, meta, "gh_discussions", date)
