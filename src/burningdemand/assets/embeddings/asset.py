# burningdemand_dagster/assets/embeddings/asset.py
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    asset,
)

from burningdemand.utils.config import config
from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.embedding_resource import EmbeddingResource


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=[
        "classifications",
    ],
    automation_condition=AutomationCondition.eager()
    .without(AutomationCondition.in_latest_time_window())
    .without(~AutomationCondition.any_deps_missing())
    .with_label("eager_allow_missing"),
    description="Generate vector embeddings for raw items using sentence transformers. Converts title+body text into 384-dimensional vectors for semantic similarity and clustering.",
)
def embeddings(
    context: AssetExecutionContext,
    db: DuckDBResource,
    embedding: EmbeddingResource,
) -> MaterializeResult:
    date = context.partition_key

    # Clear previous embeddings for this date to keep things clean
    db.execute("DELETE FROM silver.embeddings WHERE embedding_date = ?", [date])

    # Load raw items for this date, joined with classifications.
    # Only embed items with pain >= threshold (classification gate before clustering).
    pain_threshold = config.classification.pain_threshold
    items = db.query_df(
        """
        SELECT b.*
        FROM bronze.raw_items b
        JOIN silver.classifications pc
          ON b.url_hash = pc.url_hash
          AND pc.classification_date = ?
        WHERE CAST(b.created_at AS DATE) = ?
          AND pc.pain_prob >= ?
        """,
        [date, date, pain_threshold],
    )

    if len(items) == 0:
        context.log.info("No new items to process for %s", date)
        return MaterializeResult(metadata={"embeddings": 0})

    total = 0
    batch_size = config.embeddings.asset_batch_size
    total_batches = (len(items) + batch_size - 1) // batch_size

    context.log.info(
        f"Processing {len(items)} items in {total_batches} batches (batch size: {batch_size})"
    )

    for i in range(0, len(items), batch_size):
        batch_num = (i // batch_size) + 1
        batch = items.iloc[i : i + batch_size].copy()

        context.log.info(
            f"Batch {batch_num}/{total_batches}: Generating embeddings for {len(batch)} items..."
        )

        texts = (
            (
                batch["title"].fillna("").astype(str)
                + " "
                + batch["body"].fillna("").astype(str)
            )
            .str.strip()
            .tolist()
        )

        embs = embedding.encode(texts)  # ndarray (n, 384)
        # store embedding as list[float] per row (DuckDB FLOAT[384])
        batch["embedding"] = [e.tolist() for e in embs]
        batch["embedding_date"] = date

        silver_df = pd.DataFrame(
            {
                "url_hash": batch["url_hash"],
                "embedding": batch["embedding"],
                "embedding_date": batch["embedding_date"],
            }
        )

        # Convert string columns to object dtype for DuckDB compatibility
        silver_df["url_hash"] = silver_df["url_hash"].astype("object")
        silver_df["embedding_date"] = silver_df["embedding_date"].astype("object")

        context.log.info(
            f"Batch {batch_num}/{total_batches}: Inserting {len(silver_df)} items into database..."
        )

        inserted_attempt = db.upsert_df(
            "silver",
            "embeddings",
            silver_df,
            ["url_hash", "embedding", "embedding_date"],
        )
        total += int(inserted_attempt)

        context.log.info(
            f"Batch {batch_num}/{total_batches}: Completed ({inserted_attempt} upserted, {total} total embeddings)"
        )

    return MaterializeResult(
        metadata={"embeddings": int(total), "batch_size": int(batch_size)}
    )
