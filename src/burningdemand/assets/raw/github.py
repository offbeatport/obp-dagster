"""GitHub-specific parsing helpers for raw items."""

from typing import Any, Dict, List

from burningdemand.assets.raw.model import RawComment, RawReactionsGroup


def get_reactions_groups(node: Dict[str, Any]) -> List[RawReactionsGroup]:
    return [
        RawReactionsGroup(
            type=r.get("content") or r.get("type") or "",
            count=(r.get("reactors") or r.get("users") or {}).get("totalCount") or 0,
        )
        for r in node.get("reactionGroups") or []
    ]


def get_comments(parent: Dict[str, Any]) -> List[RawComment]:
    comments = parent.get("comments") or {}
    return [
        RawComment(
            body=c.get("body") or "",
            updated_at=c.get("updatedAt") or "",
            reactions=get_reactions_groups(c),
        )
        for c in comments.get("nodes") or []
    ]
