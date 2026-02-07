# burningdemand_dagster/assets/clusters.py
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
from burningdemand.utils.config import config
from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource


def clear_cluster_data_for_date(db: DuckDBResource, date: str) -> None:
    """Remove existing cluster_assignments and clusters for this partition (enables rematerialization)."""
    db.execute("DELETE FROM silver.cluster_assignments WHERE cluster_date = ?", [date])
    db.execute("DELETE FROM silver.clusters WHERE cluster_date = ?", [date])


def load_embeddings_in_window(
    db: DuckDBResource, window_start: str, date: str
) -> Tuple[Optional[pd.DataFrame], Optional[np.ndarray]]:
    """Load items with embeddings in [window_start, date]. Returns (data, embeddings_array) or (None, None) if empty."""
    data = db.query_df(
        """
        SELECT
            e.url_hash,
            e.embedding,
            b.vote_count,
            b.reaction_count,
            b.comment_count,
        FROM silver.embeddings e
        JOIN bronze.raw_items b ON e.url_hash = b.url_hash
        WHERE b.collection_date >= ?
          AND b.collection_date <= ?
        """,
        [window_start, date],
    )
    if len(data) == 0:
        return None, None
    embeddings_array = np.array(data["embedding"].tolist(), dtype=np.float32)
    return data, embeddings_array


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


def compute_cluster_metrics(
    data: pd.DataFrame,
    embeddings_array: np.ndarray,
    labels: np.ndarray,
) -> Tuple[dict, int, float]:
    """Compute per-cluster metrics (size, mean_distance, authority_score) and global noise/outlier_ratio."""
    noise_count = int(np.sum(labels == -1))
    outlier_ratio = float(noise_count / len(labels)) if len(labels) > 0 else 0.0
    unique_labels = np.unique(labels)
    cluster_ids = unique_labels[unique_labels != -1]

    cluster_metrics = {}
    for cid in cluster_ids:
        cluster_mask = labels == cid
        cluster_points = embeddings_array[cluster_mask]
        cluster_data = data.iloc[np.where(cluster_mask)[0]]
        centroid = np.mean(cluster_points, axis=0)
        distances = np.linalg.norm(cluster_points - centroid, axis=1)
        mean_distance = float(np.mean(distances))
        vote_sum = int(cluster_data["vote_count"].sum())
        reaction_sum = int(cluster_data["reaction_count"].sum())
        comment_sum = int(cluster_data["comment_count"].sum())
        # Combine votes and reactions with a simple weighting:
        #   authority_score = votes + 0.5 * comments + 0.5 * reactions
        authority_score = vote_sum + comment_sum * 0.5 + reaction_sum * 0.5
        cluster_metrics[int(cid)] = {
            "size": int(np.sum(cluster_mask)),
            "mean_distance": mean_distance,
            "authority_score": float(authority_score),
            "vote_sum": vote_sum,
            "reaction_sum": reaction_sum,
            "comment_sum": comment_sum,
        }
    return cluster_metrics, noise_count, outlier_ratio


def persist_clusters(
    db: DuckDBResource,
    date: str,
    data: pd.DataFrame,
    labels: np.ndarray,
    cluster_metrics: dict,
    outlier_ratio: float,
) -> int:
    """Write cluster_assignments and cluster metadata to DB. Returns number of cluster rows written."""
    assignments_df = pd.DataFrame(
        [
            {
                "url_hash": data["url_hash"].iloc[i],
                "cluster_date": date,
                "cluster_id": int(labels[i]),
            }
            for i in range(len(data))
        ]
    )
    db.upsert_df(
        "silver",
        "cluster_assignments",
        assignments_df,
        ["url_hash", "cluster_date", "cluster_id"],
    )
    for cid, metrics in cluster_metrics.items():
        db.execute(
            """
            INSERT INTO silver.clusters (
                cluster_date, cluster_id, cluster_size,
                outlier_ratio, mean_distance, authority_score
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (cluster_date, cluster_id) DO UPDATE SET
                cluster_size = EXCLUDED.cluster_size,
                outlier_ratio = EXCLUDED.outlier_ratio,
                mean_distance = EXCLUDED.mean_distance,
                authority_score = EXCLUDED.authority_score
            """,
            [
                date,
                cid,
                metrics["size"],
                outlier_ratio,
                metrics["mean_distance"],
                metrics["authority_score"],
            ],
        )
    return len(cluster_metrics)


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=["embeddings"],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Cluster items by similarity using HDBSCAN on stored embeddings. Uses rolling window (last 7 days) to allow clusters to grow day-over-day and detect heat signals. Groups similar issues together, allows noise/unclustered items, and stores quality metrics (outlier_ratio, mean_distance, authority_score). Authority score = sum of votes + 0.5 * comments to distinguish high-engagement signals from noise.",
)
def clusters(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    window_days = (
        config.clustering.rolling_window_days - 1
    )  # e.g. 7 days = 6 days before + partition day
    window_start = (date_obj - timedelta(days=window_days)).strftime("%Y-%m-%d")

    context.log.info(
        f"Clustering rolling window: {window_start} to {date} ({config.clustering.rolling_window_days} days)"
    )

    clear_cluster_data_for_date(db, date)
    data, embeddings_array = load_embeddings_in_window(db, window_start, date)

    if data is None or embeddings_array is None:
        return MaterializeResult(
            metadata={
                "clusters": 0,
                "items": 0,
                "noise": 0,
                "status": "no_items_in_window",
            }
        )

    context.log.info(f"Clustering {len(data)} items with stored embeddings...")
    labels = run_hdbscan(embeddings_array)
    cluster_metrics, noise_count, outlier_ratio = compute_cluster_metrics(
        data, embeddings_array, labels
    )
    created_clusters = persist_clusters(
        db, date, data, labels, cluster_metrics, outlier_ratio
    )

    sizes = [m["size"] for m in cluster_metrics.values()] if cluster_metrics else [0]
    sizes_sorted = sorted(sizes) if sizes else [0]
    p50 = sizes_sorted[len(sizes_sorted) // 2] if sizes_sorted else 0
    p90 = (
        sizes_sorted[int(len(sizes_sorted) * 0.9) - 1]
        if len(sizes_sorted) >= 10
        else sizes_sorted[-1] if sizes_sorted else 0
    )
    authority_scores = (
        [m["authority_score"] for m in cluster_metrics.values()]
        if cluster_metrics
        else [0]
    )

    return MaterializeResult(
        metadata={
            "items": int(len(data)),
            "clusters": int(created_clusters),
            "noise": int(noise_count),
            "outlier_ratio": float(outlier_ratio),
            "cluster_size_min": int(min(sizes)) if sizes else 0,
            "cluster_size_p50": int(p50),
            "cluster_size_p90": int(p90),
            "cluster_size_max": int(max(sizes)) if sizes else 0,
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
