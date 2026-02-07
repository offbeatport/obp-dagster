"""Raw GitHub issues asset."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.utils.config import config

from burningdemand.utils.raw_utils import gh_to_raw_item, materialize_raw


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
    node_fragment = (
        "... on Issue { id databaseId url title body createdAt "
        "repository { nameWithOwner } comments { totalCount } "
        "reactions { totalCount } }"
    )
    query_suffix = (
        f"is:issue comments:>{config.resources.github.min_comments} "
        f"reactions:>{config.resources.github.min_reactions}"
    )
    raw_items, meta = await github.search(
        date,
        node_fragment,
        query_suffix=query_suffix,
        hour_splits=config.resources.github.queries_per_day,
    )
    max_body = config.labeling.max_body_length_for_snippet
    items = [
        item
        for item in (gh_to_raw_item(d, "issue", max_body) for d in raw_items)
        if item is not None
    ]
    return await materialize_raw(db, items, meta, "gh_issues", date)
