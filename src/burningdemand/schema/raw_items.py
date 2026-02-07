"""Schema for raw collected items: RawItem and CollectedItems (bronze.raw_items)."""

import dataclasses
from typing import Any, Dict, List

import pandas as pd

from burningdemand.utils.url import normalize_url, url_hash


@dataclasses.dataclass
class RawItem:
    """One collected item. All sources must provide all fields (use "" for org_name/product_name when N/A)."""

    url: str = ""
    title: str = ""
    body: str = ""
    created_at: str = ""
    source_post_id: str = ""
    comment_count: int = 0
    vote_count: int = 0
    post_type: str = "issue"
    reaction_count: int = 0
    org_name: str = ""
    product_name: str = ""


# Column order matching bronze.raw_items (RawItem fields + url_hash, source, collection_date)
_TO_DF_COLUMNS = [
    "url",
    "title",
    "body",
    "created_at",
    "source_post_id",
    "comment_count",
    "vote_count",
    "post_type",
    "reaction_count",
    "org_name",
    "product_name",
    "url_hash",
    "source",
    "collection_date",
]


class CollectedItems:
    """Items plus metadata from a collection run. Converts to DataFrame for bronze.raw_items."""

    def __init__(self, items: List[RawItem], meta: Dict[str, Any]) -> None:
        self.items = items
        self.meta = meta

    def to_df(self, source: str, date: str) -> pd.DataFrame:
        """Build a normalized DataFrame for upsert into bronze.raw_items.
        Returns an empty DataFrame with correct columns when there are no items.
        """
        if not self.items:
            return pd.DataFrame(columns=_TO_DF_COLUMNS)

        df = pd.DataFrame([dataclasses.asdict(i) for i in self.items])
        df["url"] = df["url"].map(normalize_url)
        df["url_hash"] = df["url"].map(url_hash)
        df["source"] = source
        df["collection_date"] = date

        df["title"] = df["title"].fillna("").astype("object")
        df["body"] = df["body"].fillna("").astype("object")
        df["url"] = df["url"].astype("object")
        df["url_hash"] = df["url_hash"].astype("object")
        df["source"] = df["source"].astype("object")
        df["source_post_id"] = df["source_post_id"].fillna("").astype("object")
        df["org_name"] = df["org_name"].fillna("").astype("object")
        df["product_name"] = df["product_name"].fillna("").astype("object")
        df["post_type"] = df["post_type"].fillna("issue").astype("object")
        df["collection_date"] = pd.to_datetime(
            df["collection_date"], format="%Y-%m-%d", errors="coerce"
        ).dt.date
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["comment_count"] = df["comment_count"].fillna(0).astype(int)
        df["vote_count"] = df["vote_count"].fillna(0).astype(int)
        df["reaction_count"] = df["reaction_count"].fillna(0).astype(int)
        return df
