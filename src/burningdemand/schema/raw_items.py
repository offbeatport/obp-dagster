"""Schema for raw collected items: RawItem and CollectedItems (bronze.raw_items)."""

import dataclasses
import json
from typing import Any, Dict, List

import pandas as pd

from burningdemand.utils.url import normalize_url, url_hash


@dataclasses.dataclass
class RawReaction:
    type: str = ""
    count: int = 0


@dataclasses.dataclass
class RawComment:
    body: str = ""
    created_at: str = ""
    votes_count: int = 0
    reactions: List[RawReaction] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class RawItem:
    """One collected item. All sources must provide all fields (use "" for org_name/product_name when N/A)."""

    url: str = ""
    title: str = ""
    body: str = ""
    created_at: str = ""
    source_post_id: str = ""
    comments_list: List[RawComment] = dataclasses.field(default_factory=list)
    comments_count: int = 0
    votes_count: int = 0
    post_type: str = "issue"
    reactions_groups: List[RawReaction] = dataclasses.field(default_factory=list)
    reactions_count: int = 0
    org_name: str = ""
    product_name: str = ""


class CollectedItems:
    """Items plus metadata from a collection run. Converts to DataFrame for bronze.raw_items."""

    def __init__(self, items: List[RawItem], meta: Dict[str, Any]) -> None:
        self.items = items
        self.meta = meta

    def to_df(self, source: str, date: str) -> pd.DataFrame:
        """Build a normalized DataFrame for upsert into bronze.raw_items."""
        if not self.items:
            return pd.DataFrame()

        df = pd.DataFrame([dataclasses.asdict(i) for i in self.items])
        df["url"] = df["url"].map(normalize_url)
        df["url_hash"] = df["url"].map(url_hash)
        df["source"] = source
        df["collected_at"] = date

        df["title"] = df["title"].fillna("").astype("object")
        df["body"] = df["body"].fillna("").astype("object")
        df["url"] = df["url"].astype("object")
        df["url_hash"] = df["url_hash"].astype("object")
        df["source"] = df["source"].astype("object")
        df["source_post_id"] = df["source_post_id"].fillna("").astype("object")
        df["org_name"] = df["org_name"].fillna("").astype("object")
        df["product_name"] = df["product_name"].fillna("").astype("object")
        df["post_type"] = df["post_type"].fillna("issue").astype("object")
        df["collected_at"] = pd.to_datetime(
            df["collected_at"], format="%Y-%m-%d", errors="coerce"
        ).dt.date
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["comments_list"] = df["comments_list"].apply(json.loads)
        df["comments_count"] = df["comments_count"].fillna(0).astype(int)
        df["reactions_groups"] = df["reactions_groups"].apply(json.loads)
        df["reactions_count"] = df["reactions_count"].fillna(0).astype(int)
        df["votes_count"] = df["votes_count"].fillna(0).astype(int)
        df["reactions_count"] = df["reactions_count"].fillna(0).astype(int)
        return df
