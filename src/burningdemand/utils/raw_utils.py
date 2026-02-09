"""Shared helpers for raw assets (bronze.raw_items)."""

from typing import Any, Dict, List

from dagster import MaterializeResult

from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.schema.raw_items import CollectedItems, RawItem


def gh_to_raw_item(d: Dict[str, Any], post_type: str) -> RawItem:
    """Convert GitHub REST item or GraphQL node to RawItem."""
    url = d.get("url") or d.get("html_url") or ""
    parts = url.rstrip("/").replace("https://github.com/", "").split("/")[:2]
    org, product = (parts + ["", ""])[:2]
    body = d.get("body") or ""
    created = d.get("createdAt") or d.get("created_at") or ""
    sid = d.get("databaseId") or d.get("number") or d.get("id")
    source_id = str(sid) if sid is not None else ""
    comments = d.get("comments")
    comment_count = (
        comments
        if isinstance(comments, int)
        else (comments or {}).get("totalCount", 0) or 0
    )
    reactions = d.get("reactions") or {}
    reaction_count = reactions.get("total_count") or reactions.get("totalCount") or 0
    return RawItem(
        url=url,
        title=(d.get("title") or ""),
        body=body,
        created_at=created,
        source_post_id=source_id,
        comments_list=[],
        comments_count=comment_count,
        upvotes_count=0,
        post_type=post_type,
        reactions_count=reaction_count,
        org_name=org,
        product_name=product,
    )


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
    inserted_attempt = db.upsert_df("bronze", "raw_items", df)
    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": len(df),
            "insert_attempted": inserted_attempt,
            "collector": meta,
        }
    )
