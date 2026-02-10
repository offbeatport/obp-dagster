"""Schema for raw collected items: RawItem and CollectedItems (bronze.raw_items)."""

import dataclasses
import json
from typing import Any, Dict, List

import pandas as pd

from burningdemand.utils.url import normalize_url, url_hash


@dataclasses.dataclass
class RawReactionsGroups:
    type: str = ""
    count: int = 0


@dataclasses.dataclass
class RawComment:
    body: str = ""
    updated_at: str = ""
    reactions: List[RawReactionsGroups] = dataclasses.field(default_factory=list)


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
    upvotes_count: int = 0
    post_type: str = "issue"
    reactions_groups: List[RawReactionsGroups] = dataclasses.field(default_factory=list)
    reactions_count: int = 0
    org_name: str = ""
    product_name: str = ""
    product_desc: str = ""
    product_stars: int = 0
    product_forks: int = 0
    product_watchers: int = 0
    license: str = ""
    labels: List[str] = dataclasses.field(default_factory=list)


# Column order for bronze.raw_items (must match duckdb.sql). Used by to_df() and by upsert when columns=None.
RAW_ITEMS_TABLE_COLUMNS = [
    "url",
    "url_hash",
    "source",
    "source_post_id",
    "post_type",
    "org_name",
    "product_name",
    "product_desc",
    "product_stars",
    "product_forks",
    "product_watchers",
    "license",
    "title",
    "body",
    "upvotes_count",
    "reactions_groups",
    "reactions_count",
    "comments_list",
    "comments_count",
    "created_at",
    "collected_at",
    "labels",
]


class CollectedItems:
    """Items plus metadata from a collection run. Converts to DataFrame for bronze.raw_items."""

    def __init__(self, items: List[RawItem], meta: Dict[str, Any]) -> None:
        self.items = items
        self.meta = meta

    def to_df(self, source: str, date: str) -> pd.DataFrame:
        """Build a normalized DataFrame for upsert into bronze.raw_items."""
        if not self.items:
            return pd.DataFrame(columns=RAW_ITEMS_TABLE_COLUMNS)

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
        df["product_desc"] = df["product_desc"].fillna("").astype("object")
        df["product_stars"] = df["product_stars"].fillna(0).astype(int)
        df["product_forks"] = df["product_forks"].fillna(0).astype(int)
        df["product_watchers"] = df["product_watchers"].fillna(0).astype(int)
        df["license"] = df["license"].fillna("").astype("object")
        df["post_type"] = df["post_type"].fillna("issue").astype("object")
        df["collected_at"] = pd.to_datetime(
            df["collected_at"], format="%Y-%m-%d", errors="coerce"
        ).dt.date
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["comments_list"] = df["comments_list"].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )
        df["comments_count"] = df["comments_count"].fillna(0).astype(int)
        df["reactions_groups"] = df["reactions_groups"].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )
        df["reactions_count"] = df["reactions_count"].fillna(0).astype(int)
        df["upvotes_count"] = df["upvotes_count"].fillna(0).astype(int)
        df["labels"] = df["labels"].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else x
        )
        return df[RAW_ITEMS_TABLE_COLUMNS]
