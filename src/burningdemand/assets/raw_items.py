import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import source_day_partitions
from burningdemand.resources.collectors.collectors_resource import CollectorsResource
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.url import normalize_url, url_hash


@asset(
    partitions_def=source_day_partitions,
    group_name="bronze",
    pool="source_api_rate_limit",
    description="Collect raw issues from external sources (GitHub, StackOverflow, Reddit, HackerNews). Stores title, body, URL, and metadata for each collected item.",
)
async def raw_items(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    source = context.partition_key.keys_by_dimension["source"]
    date = context.partition_key.keys_by_dimension["date"]

    items, meta = await collector.collect(source, date)

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

    df = pd.DataFrame(items)
    df["url"] = df["url"].map(normalize_url)
    df["url_hash"] = df["url"].map(url_hash)
    df["source"] = source
    df["collection_date"] = date

    # enforce expected cols & sizes - convert to object dtype for DuckDB compatibility
    df["title"] = df["title"].fillna("").astype("object")
    df["body"] = df["body"].fillna("").astype("object")
    df["url"] = df["url"].astype("object")
    df["url_hash"] = df["url_hash"].astype("object")
    df["source"] = df["source"].astype("object")
    df["collection_date"] = df["collection_date"].astype("object")
    df["created_at"] = df["created_at"].fillna("").astype("object")

    # Handle comment_count and vote_count (default to 0 if not present)
    if "comment_count" not in df.columns:
        df["comment_count"] = 0
    if "vote_count" not in df.columns:
        df["vote_count"] = 0
    df["comment_count"] = df["comment_count"].fillna(0).astype(int)
    df["vote_count"] = df["vote_count"].fillna(0).astype(int)

    inserted_attempt = db.upsert_df(
        "bronze",
        "raw_items",
        df,
        [
            "url_hash",
            "source",
            "collection_date",
            "url",
            "title",
            "body",
            "created_at",
            "comment_count",
            "vote_count",
        ],
    )

    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": int(len(df)),
            "insert_attempted": int(inserted_attempt),
            "collector": meta,
        }
    )
