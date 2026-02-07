"""Day-partitioned raw assets per source. All write into bronze.raw_items."""

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.collectors.collectors_resource import CollectorsResource
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.url import normalize_url, url_hash

# Asset key -> source value stored in bronze.raw_items
SOURCE_BY_ASSET = {
    "gh_issues": "github",
    "gh_discussions": "github",
    "rd": "reddit",
    "so": "stackoverflow",
    "hn": "hackernews",
}

UPSERT_COLUMNS = [
    "source",
    "source_post_id",
    "post_type",
    "url_hash",
    "collection_date",
    "url",
    "title",
    "body",
    "created_at",
    "comment_count",
    "vote_count",
    "org_name",
    "product_name",
    "reaction_count",
]


async def _materialize_raw(
    db: DuckDBResource,
    collector: CollectorsResource,
    asset_key: str,
    date: str,
) -> MaterializeResult:
    """Collect for (asset_key, date) and upsert into bronze.raw_items."""
    items, meta = await collector.collect_by_asset(asset_key, date)
    source = SOURCE_BY_ASSET[asset_key]

    if not items:
        return MaterializeResult(
            metadata={
                "asset_key": asset_key,
                "source": source,
                "date": date,
                "collected": 0,
                "insert_attempted": 0,
                "collector": meta,
            }
        )

    df = pd.DataFrame(items)
    df["url"] = df["url"].map(normalize_url)
    df["url_hash"] = df["url"].map(url_hash)
    df["source"] = source
    df["collection_date"] = date
    df["product_name"] = df["product_name"].fillna("").astype("obj")
    df["org_name"] = df["org_name"].fillna("").astype("obj")
    df["title"] = df["title"].fillna("").astype("object")
    df["body"] = df["body"].fillna("").astype("object")
    df["url"] = df["url"].astype("object")
    df["url_hash"] = df["url_hash"].astype("object")
    df["source"] = df["source"].astype("object")
    df["source_post_id"] = df.get("source_post_id", "").fillna("").astype("object")
    df["org_name"] = df.get("org_name", "").fillna("").astype("object")
    df["product_name"] = df.get("product_name", "").fillna("").astype("object")
    df["post_type"] = df.get("post_type", "issue").fillna("issue").astype("object")
    df["collection_date"] = pd.to_datetime(
        df["collection_date"], format="%Y-%m-%d", errors="coerce"
    ).dt.date
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["comment_count"] = df["comment_count"].fillna(0).astype(int)
    df["vote_count"] = df["vote_count"].fillna(0).astype(int)
    df["reaction_count"] = df.get("reaction_count", 0).fillna(0).astype(int)

    inserted_attempt = db.upsert_df(
        "bronze",
        "raw_items",
        df,
        UPSERT_COLUMNS,
    )

    return MaterializeResult(
        metadata={
            "asset_key": asset_key,
            "source": source,
            "date": date,
            "collected": int(len(df)),
            "insert_attempted": int(inserted_attempt),
            "collector": meta,
        }
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub issues per day. Writes into bronze.raw_items (source=github, post_type=issue).",
)
async def raw_gh_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    date = context.partition_key
    return await _materialize_raw(context, db, collector, "gh_issues", date)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub discussions per day. Writes into bronze.raw_items (source=github, post_type=discussion). Stub until implemented.",
)
async def raw_gh_discussions(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    date = context.partition_key
    return await _materialize_raw(context, db, collector, "gh_discussions", date)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Reddit posts per day. Writes into bronze.raw_items (source=reddit, post_type=post).",
)
async def raw_rd(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    date = context.partition_key
    return await _materialize_raw(context, db, collector, "rd", date)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Stack Overflow questions per day. Writes into bronze.raw_items (source=stackoverflow, post_type=question).",
)
async def raw_so(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    date = context.partition_key
    return await _materialize_raw(context, db, collector, "so", date)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw Hacker News stories per day. Writes into bronze.raw_items (source=hackernews, post_type=story).",
)
async def raw_hn(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    date = context.partition_key
    return await _materialize_raw(context, db, collector, "hn", date)
