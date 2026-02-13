"""Schema for raw collected items: RawItem and CollectedItems (bronze.raw_items)."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, field_validator

from burningdemand.utils.text import truncate_middle

log = logging.getLogger(__name__)

ALLOWED_LICENSES = frozenset(
    [
        "BSL-1.0",
        "UPL-1.0",
        "BlueOak-1.0.0",
        "Zlib",
        "PostgreSQL",
        "ISC",
        "ECL-2.0",
        "CC0-1.0",
        "Unlicense",
        "MS-PL",
        "Apache-2.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "BSD-3-Clause-Clear",
        "0BSD",
        "MIT",
        "MIT-0",
        "AFL-3.0",
        "NCSA",
    ]
)
HARD_FILTER_MIN_COMMENTS = 0


class RawReactionsGroup(BaseModel):
    type: str = ""
    count: int = 0


class RawComment(BaseModel):
    body: str = ""
    updated_at: str = ""
    reactions: List[RawReactionsGroup] = []


class RawItem(BaseModel):
    url: str = ""
    url_hash: str = ""
    source: str = ""
    collected_at: Optional[datetime] = None
    title: str = ""
    body: str = ""
    created_at: Optional[datetime] = None
    source_post_id: str = ""
    comments_list: List[RawComment] = []
    comments_count: Optional[int] = 0
    upvotes_count: Optional[int] = 0
    post_type: str = "issue"
    reactions_groups: List[RawReactionsGroup] = []
    reactions_count: Optional[int] = 0
    org_name: str = ""
    product_name: str = ""
    product_desc: str = ""
    product_stars: Optional[int] = 0
    product_forks: Optional[int] = 0
    product_watchers: Optional[int] = 0
    license: str = ""
    labels: List[str] = []

    @field_validator("comments_list", "reactions_groups", mode="before")
    @classmethod
    def parse_json(cls, v: Any) -> Any:
        if isinstance(v, str):
            try:
                return json.loads(v) if v else []
            except json.JSONDecodeError:
                return []
        return v

    def to_llm_prompt_text(self, max_body: int, max_comment: int) -> str:
        def fmt_reactions(groups: List[RawReactionsGroup]) -> str:
            if not groups:
                return "0"
            return ", ".join(f"{g.type}:{g.count}" for g in groups if g.type or g.count)

        labels_str = ", ".join(self.labels) if self.labels else "N/A"
        comment_lines = []
        for c in self.comments_list:
            body = truncate_middle(c.body or "", max_comment)
            rx = fmt_reactions(c.reactions)
            rx_total = sum(r.count for r in c.reactions)
            comment_lines.append(
                f"[{rx_total} reactions: {rx}]\n{body}" if rx else body
            )

        parts = [
            f"Title: {self.title or ''}",
            f"Labels: {labels_str}",
            f"Body: {truncate_middle(self.body or '', max_body)}",
            f"Product Name: {self.product_name or ''}",
            f"Product Description: {truncate_middle(self.product_desc or '', 500)}",
            f"Product Stars: {self.product_stars or 0} | Product Forks: {self.product_forks or 0} | Product Watchers: {self.product_watchers or 0}",
            f"Type: {self.source or ''}",
            f"UpVotes: {self.upvotes_count or 0}",
            f"Reactions: {self.reactions_count or 0} ({fmt_reactions(self.reactions_groups)})",
            "Comments:\n" + ("\n".join(comment_lines) if comment_lines else "N/A"),
        ]

        return "\n\n".join(parts)


class CollectedItems:
    def __init__(self, items: List[RawItem], meta: Dict[str, Any]) -> None:
        self.items = self._filter(items)
        self.meta = meta

    def _filter(self, items: List[RawItem]) -> List[RawItem]:
        kept = [i for i in items if (i.license or "").strip() in ALLOWED_LICENSES]
        kept = [i for i in kept if (i.comments_count or 0) >= HARD_FILTER_MIN_COMMENTS]
        if len(kept) < len(items):
            log.info("Filtered: %s → %s items", len(items), len(kept))
        return kept

    def to_df(self) -> pd.DataFrame:
        cols = list(RawItem.model_fields)
        if not self.items:
            return pd.DataFrame(columns=cols)
        rows = [i.model_dump() for i in self.items]
        return pd.DataFrame(rows)[cols]
