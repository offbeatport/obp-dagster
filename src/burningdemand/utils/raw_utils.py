"""Shared helpers for GitHub GraphQL structures."""

from typing import Any, Dict, Iterable, List

from dagster import MaterializeResult

from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.schema.raw_items import (
    CollectedItems,
    RawComment,
    RawItem,
    RawReactionsGroups,
)


def parse_github_reaction_groups(
    groups: Iterable[Dict[str, Any]],
) -> List[RawReactionsGroups]:
    """Convert GitHub reactionGroups[] into RawReactionsGroups list."""
    return [
        RawReactionsGroups(
            type=(rg.get("content") or ""),
            count=(rg.get("reactors") or rg.get("users") or {}).get("totalCount") or 0,
        )
        for rg in (groups or [])
    ]


def parse_github_comments_list(comments: Dict[str, Any] | None) -> List[RawComment]:
    """Convert a GitHub comments object into a list[RawComment]."""
    nodes = (comments or {}).get("nodes") or []
    return [
        RawComment(
            body=(c.get("body") or ""),
            updated_at=(c.get("updatedAt") or c.get("createdAt") or ""),
            reactions=parse_github_reaction_groups(c.get("reactionGroups") or []),
        )
        for c in nodes
    ]


def parse_github_labels(labels: Dict[str, Any] | None) -> List[str]:
    """Convert a GitHub labels object into a flat list of label names."""
    nodes = (labels or {}).get("nodes") or []
    return [n.get("name") or "" for n in nodes if (n.get("name") or "").strip()]


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
