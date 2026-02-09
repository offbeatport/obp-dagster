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

    # node_fragment = f"""
    #     ... on Discussion {{
    #         id
    #         url
    #         title
    #         body
    #         createdAt
    #         upvoteCount
    #         isAnswered
    #         closed
    #         answer {{ body }}
    #         repository {{
    #             name
    #             owner {{ login }}
    #         }}
    #         comments(last: {cfg}) {{
    #             totalCount
    #             nodes {{
    #                 body
    #                 updatedAt
    #                 reactions {{ totalCount }}
    #                 reactionGroups {{
    #                     content
    #                     reactors {{ totalCount }}
    #                 }}
    #             }}
    #         }}
    #         reactions {{ totalCount }}
    #         reactionGroups {{
    #             content
    #             reactors {{ totalCount }}
    #         }}
    #     }}
    # """
