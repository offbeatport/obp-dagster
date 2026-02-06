import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import source_day_partitions
from burningdemand.resources.collectors.collectors_resource import CollectorsResource
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.url import normalize_url, url_hash


@asset(
    partitions_def=source_day_partitions,
    group_name="bronze",
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

    # String-like columns: use object so DuckDB gets VARCHAR (pandas "string" dtype is reported as "str" and DuckDB doesn't recognize it in conn.register())
    df["title"] = df["title"].fillna("").astype("object")
    df["body"] = df["body"].fillna("").astype("object")
    df["url"] = df["url"].astype("object")
    df["url_hash"] = df["url_hash"].astype("object")
    df["source"] = df["source"].astype("object")
    df["source_post_id"] = df.get("source_post_id", "").fillna("").astype("object")
    df["org_name"] = df["org_name"].fillna("").astype("object")
    df["product_name"] = df["product_name"].fillna("").astype("object")
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
        [
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
