# burningdemand_dagster/assets/groups/asset.py
import pandas as pd
import numpy as np
import hdbscan
from datetime import datetime, timedelta
from typing import Optional, Tuple

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    MaterializeResult,
    asset,
)
from pydantic import BaseModel

from burningdemand.utils.config import config
from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource


class EmbeddingWithMetadata(BaseModel):
    """Result of embedding + raw_items join for clustering."""

    url_hash: str = ""
    embedding: list[float] = []
    vote_count: int = 0
    reactions_count: int = 0
    comments_count: int = 0


def clear_group_data_for_date(db: DuckDBResource, date: str) -> None:
    """Remove existing group_members and groups for this partition (enables rematerialization)."""
    db.execute("DELETE FROM silver.group_members WHERE group_date = ?", [date])
    db.execute("DELETE FROM silver.groups WHERE group_date = ?", [date])


def load_embeddings_in_window(
    db: DuckDBResource, window_start: str, date: str
) -> Tuple[Optional[list[EmbeddingWithMetadata]], Optional[np.ndarray]]:
    """Load items with embeddings in [window_start, date]. Returns (data, embeddings_array) or (None, None) if empty."""
    items: list[EmbeddingWithMetadata] = db.query_df(
        """
        SELECT
            e.url_hash,
            e.embedding,
            b.upvotes_count AS vote_count,
            b.reactions_count,
            b.comments_count
        FROM silver.embeddings e
        JOIN bronze.raw_items b ON e.url_hash = b.url_hash
        WHERE b.collected_at >= ?
          AND b.collected_at <= ?
        """,
        [window_start, date],
        model=EmbeddingWithMetadata,
    )
    if len(items) == 0:
        return None, None
    embeddings_array = np.array([i.embedding for i in items], dtype=np.float32)
    return items, embeddings_array


def run_hdbscan(embeddings_array: np.ndarray) -> np.ndarray:
    """Run HDBSCAN and return cluster labels (-1 = noise)."""
    n = len(embeddings_array)
    min_cluster_size = max(
        config.clustering.min_cluster_size_floor,
        n // config.clustering.min_cluster_size_divisor,
    )
    min_samples = max(
        config.clustering.min_samples_floor,
        min_cluster_size // config.clustering.min_samples_divisor,
    )
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric=config.clustering.metric,
        cluster_selection_method=config.clustering.selection_method,
    )
    return clusterer.fit_predict(embeddings_array)


def compute_group_metrics(
    data: list[EmbeddingWithMetadata],
    embeddings_array: np.ndarray,
    labels: np.ndarray,
) -> Tuple[dict, int, float]:
    """Compute per-group metrics (size, mean_distance, authority_score) and global noise/outlier_ratio."""
    noise_count = int(np.sum(labels == -1))
    outlier_ratio = float(noise_count / len(labels)) if len(labels) > 0 else 0.0
    unique_labels = np.unique(labels)
    group_ids = unique_labels[unique_labels != -1]

    group_metrics = {}
    for gid in group_ids:
        group_mask = labels == gid
        group_points = embeddings_array[group_mask]
        indices = np.where(group_mask)[0]
        group_items = [data[i] for i in indices]
        centroid = np.mean(group_points, axis=0)
        distances = np.linalg.norm(group_points - centroid, axis=1)
        mean_distance = float(np.mean(distances))
        vote_sum = sum(i.vote_count for i in group_items)
        reaction_sum = sum(i.reactions_count for i in group_items)
        comment_sum = sum(i.comments_count for i in group_items)
        authority_score = vote_sum + comment_sum * 0.5 + reaction_sum * 0.5
        group_metrics[int(gid)] = {
            "size": int(np.sum(group_mask)),
            "mean_distance": mean_distance,
            "authority_score": float(authority_score),
            "vote_sum": vote_sum,
            "reaction_sum": reaction_sum,
            "comment_sum": comment_sum,
        }
    return group_metrics, noise_count, outlier_ratio


def persist_groups(
    db: DuckDBResource,
    date: str,
    data: list[EmbeddingWithMetadata],
    labels: np.ndarray,
    group_metrics: dict,
    outlier_ratio: float,
) -> int:
    """Write group_members and group metadata to DB. Returns number of group rows written."""
    assignments_df = pd.DataFrame(
        [
            {
                "url_hash": data[i].url_hash,
                "group_date": date,
                "group_id": int(labels[i]),
            }
            for i in range(len(data))
        ]
    )
    db.upsert_df(
        "silver",
        "group_members",
        assignments_df,
        ["url_hash", "group_date", "group_id"],
    )
    for gid, metrics in group_metrics.items():
        db.execute(
            """
            INSERT INTO silver.groups (
                group_date, group_id, group_size,
                outlier_ratio, mean_distance, authority_score
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (group_date, group_id) DO UPDATE SET
                group_size = EXCLUDED.group_size,
                outlier_ratio = EXCLUDED.outlier_ratio,
                mean_distance = EXCLUDED.mean_distance,
                authority_score = EXCLUDED.authority_score
            """,
            [
                date,
                gid,
                metrics["size"],
                outlier_ratio,
                metrics["mean_distance"],
                metrics["authority_score"],
            ],
        )
    return len(group_metrics)


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=["embeddings"],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Group items by similarity using HDBSCAN on stored embeddings. Uses rolling window to allow groups to grow day-over-day. Groups similar issues together, allows noise, stores quality metrics.",
)
def groups(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    window_days = config.clustering.rolling_window_days - 1
    window_start = (date_obj - timedelta(days=window_days)).strftime("%Y-%m-%d")

    context.log.info(
        f"Grouping rolling window: {window_start} to {date} ({config.clustering.rolling_window_days} days)"
    )

    clear_group_data_for_date(db, date)
    data, embeddings_array = load_embeddings_in_window(db, window_start, date)

    if data is None or embeddings_array is None:
        return MaterializeResult(
            metadata={
                "groups": 0,
                "items": 0,
                "noise": 0,
                "window_start": window_start,
                "window_end": date,
                "status": "no_items_in_window",
            }
        )

    context.log.info("Grouping %s items with stored embeddings...", len(data))
    labels = run_hdbscan(embeddings_array)
    group_metrics, noise_count, outlier_ratio = compute_group_metrics(
        data, embeddings_array, labels
    )
    created_groups = persist_groups(
        db, date, data, labels, group_metrics, outlier_ratio
    )

    sizes = [m["size"] for m in group_metrics.values()] if group_metrics else [0]
    sizes_sorted = sorted(sizes) if sizes else [0]
    p50 = sizes_sorted[len(sizes_sorted) // 2] if sizes_sorted else 0
    p90 = (
        sizes_sorted[int(len(sizes_sorted) * 0.9) - 1]
        if len(sizes_sorted) >= 10
        else sizes_sorted[-1] if sizes_sorted else 0
    )
    authority_scores = (
        [m["authority_score"] for m in group_metrics.values()] if group_metrics else [0]
    )

    return MaterializeResult(
        metadata={
            "items": int(len(data)),
            "groups": int(created_groups),
            "noise": int(noise_count),
            "outlier_ratio": float(outlier_ratio),
            "group_size_min": int(min(sizes)) if sizes else 0,
            "group_size_p50": int(p50),
            "group_size_p90": int(p90),
            "group_size_max": int(max(sizes)) if sizes else 0,
            "authority_score_max": (
                float(max(authority_scores)) if authority_scores else 0.0
            ),
            "authority_score_avg": (
                float(np.mean(authority_scores)) if authority_scores else 0.0
            ),
            "window_start": window_start,
            "window_end": date,
        }
    )
