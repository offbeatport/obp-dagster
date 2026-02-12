"""Schema for raw collected items: RawItem and CollectedItems (bronze.raw_items)."""

import json
import logging
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
    collected_at: str = ""
    title: str = ""
    body: str = ""
    created_at: str = ""
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

    def to_text(self, max_body: int, max_comment: int) -> str:
        parts = [
            f"Title: {self.title or ''}",
            f"Body: {truncate_middle(self.body or '', max_body)}",
        ]
        comments = "\n".join(
            truncate_middle(c.body or "", max_comment) for c in self.comments_list
        )
        if comments:
            parts.append(f"Comments:\n{comments}")
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
