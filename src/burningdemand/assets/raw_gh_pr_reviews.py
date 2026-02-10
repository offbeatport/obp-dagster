"""Raw GitHub PR reviews asset."""

from typing import Any, Dict

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawComment, RawItem, RawReactionsGroups
from burningdemand.utils.config import config
from burningdemand.utils.raw_utils import materialize_raw


def _parse_reaction_groups(groups: list[Dict[str, Any]]) -> list[RawReactionsGroups]:
    return [
        RawReactionsGroups(
            type=(rg.get("content") or ""),
            count=(rg.get("reactors") or {}).get("totalCount") or 0,
        )
        for rg in groups
    ]


def _pr_reviews_to_raw_item(d: Dict[str, Any]) -> RawItem:
    repository = d.get("repository") or {}
    repo = repository.get("nameWithOwner") or ""
    org, product = (repo.split("/", 1) + [""])[:2]
    license_info = repository.get("licenseInfo") or {}
    license_name = (
        license_info.get("spdxId")
        or license_info.get("key")
        or license_info.get("name")
        or ""
    )
    reviews = (d.get("reviews") or {}).get("nodes") or []
    review_comments = [
        RawComment(
            body=(
                f"[{(r.get('state') or '').strip()}] "
                f"{((r.get('author') or {}).get('login') or 'unknown')}: "
                f"{(r.get('body') or '').strip()}"
            ).strip(),
            created_at=(r.get("submittedAt") or ""),
            upvotes_count=0,
            reactions=[],
        )
        for r in reviews
        if (r.get("body") or "").strip()
    ]

    body = d.get("body") or ""
    review_meta = (
        f"\n\n[Review Signals] review_count={(d.get('reviews') or {}).get('totalCount', 0)}, "
        f"comment_count={(d.get('comments') or {}).get('totalCount', 0)}, "
        f"state={d.get('state')}, merged={d.get('merged')}"
    )
    return RawItem(
        url=d.get("url") or "",
        title=d.get("title") or "",
        body=f"{body}{review_meta}".strip(),
        created_at=d.get("createdAt") or "",
        source_post_id=str(d.get("id") or ""),
        comments_list=review_comments,
        comments_count=(d.get("reviews") or {}).get("totalCount") or 0,
        upvotes_count=0,
        post_type="pr_review",
        reactions_groups=_parse_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=(d.get("reactions") or {}).get("totalCount") or 0,
        org_name=org,
        product_name=product,
        license=license_name,
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub PR reviews per day. Writes into bronze.raw_items (source=gh_pr_reviews, post_type=pr_review).",
)
async def raw_gh_pr_reviews(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:
    date = context.partition_key
    cfg = config.raw_gh_pr_reviews

    node_fragment = f"""
        ... on PullRequest {{
            id
            url
            title
            body
            createdAt
            state
            merged
            repository {{ 
                nameWithOwner
                licenseInfo {{ spdxId name }}
            }}
            comments {{ totalCount }}
            reviews(last: {cfg.max_reviews}) {{
                totalCount
                nodes {{
                    body
                    state
                    submittedAt
                    author {{ login }}
                }}
            }}
            reactions {{ totalCount }}
            reactionGroups {{
                content
                reactors {{ totalCount }}
            }}
        }}
    """
    # Main slice: PRs with enough reviews
    query_suffix = f"is:pr review:>={cfg.min_reviews} sort:updated-desc"
    raw_items, meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=query_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )

    # Extra slice: PRs marked as "won't fix"/not planned, even with few/no reviews
    wontfix_suffix = "is:pr sort:updated-desc label:wontfix reason:not_planned"
    wontfix_items, wontfix_meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=wontfix_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )

    by_id: dict[str, Dict[str, Any]] = {d["id"]: d for d in raw_items}
    for d in wontfix_items:
        by_id.setdefault(d["id"], d)

    all_items = list(by_id.values())
    items = [_pr_reviews_to_raw_item(d) for d in all_items]
    return await materialize_raw(db, items, meta, "gh_pr_reviews", date)

