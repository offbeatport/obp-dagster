"""Raw GitHub discussions asset."""

from typing import Any, Dict, List

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawComment, RawItem, RawReactionsGroups
from burningdemand.utils.config import config
from burningdemand.utils.raw_utils import materialize_raw


def _parse_reaction_groups(groups: List[Dict[str, Any]]) -> List[RawReactionsGroups]:
    return [
        RawReactionsGroups(
            type=(rg.get("content") or ""),
            count=(rg.get("users") or {}).get("totalCount") or 0,
        )
        for rg in groups
    ]


def gh_to_raw_item(d: Dict[str, Any], post_type: str) -> RawItem:
    """Convert GitHub GQL node to RawItem."""
    repository = d.get("repository") or {}
    license_info = repository.get("licenseInfo") or {}
    license_name = license_info.get("spdxId") or ""

    return RawItem(
        url=d.get("url"),
        title=d.get("title") or "",
        body=d.get("body") or "",
        org_name=repository.get("name") or "",
        product_name=(repository.get("owner") or {}).get("login", ""),
        product_stars=repository.get("stargazerCount") or 0,
        product_forks=repository.get("forkCount") or 0,
        product_watchers=(repository.get("watchers") or {}).get("totalCount") or 0,
        license=license_name,
        created_at=d.get("createdAt") or "",
        comments_list=[
            RawComment(
                body=(c.get("body") or ""),
                created_at=(c.get("updatedAt") or c.get("createdAt") or ""),
                upvotes_count=(c.get("reactions") or {}).get("totalCount") or 0,
                reactions=_parse_reaction_groups(c.get("reactionGroups") or []),
            )
            for c in d.get("comments").get("nodes", [])
        ],
        comments_count=d.get("comments").get("totalCount") or 0,
        upvotes_count=d.get("upvoteCount") or 0,
        post_type=post_type,
        reactions_groups=_parse_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=(d.get("reactions") or {}).get("totalCount") or 0,
        source_post_id=str(d.get("id")) or "",
    )


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
    cfg = config.raw_gh_discussions

    node_fragment = f"""
        ... on Discussion {{
            id              
            url 
            title 
            body 
            createdAt
            upvoteCount
            isAnswered
            closed
            answer {{ body }}
            repository {{
                name        
                owner {{ login }}
                licenseInfo {{ spdxId }}
            }}
            comments(last: {cfg.max_comments}) {{
                totalCount
                nodes {{
                    body
                    updatedAt
                    reactions {{ totalCount }}
                    reactionGroups {{
                        content        
                        reactors {{ totalCount }}
                    }}
                }}
            }} 
            reactions {{ totalCount }} 
            reactionGroups {{
                content        
                reactors {{ totalCount }}
            }}
        }}
    """

    query_suffix = f"comments:>={cfg.min_comments} sort:interactions-desc"

    raw_items, meta = await github.search(
        date,
        node_fragment,
        query_suffix=query_suffix,
        type="DISCUSSION",
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )

    items = [item for item in (gh_to_raw_item(d, "discussion") for d in raw_items)]

    return await materialize_raw(db, items, meta, "gh_discussions", date)
