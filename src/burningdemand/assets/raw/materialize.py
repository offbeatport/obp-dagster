"""Shared helpers for raw data collection: GitHub parsing and materialization."""

from typing import Any, Dict, List

from dagster import MaterializeResult

from burningdemand.resources.duckdb_resource import DuckDBResource

from .model import CollectedItems, RawItem


async def materialize_raw(
    db: DuckDBResource,
    items: List[RawItem],
    meta: Dict[str, Any],
    source: str,
    date: str,
) -> MaterializeResult:
    """Upsert collected items into bronze.raw_items. Returns empty result if no items."""
    # Delete existing rows for this source and date before insert (keep things clean)
    db.execute(
        "DELETE FROM bronze.raw_items WHERE source = ? AND CAST(collected_at AS DATE) = ?",
        [source, date],
    )

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
    items = [i.model_copy(update={"source": source, "collected_at": date}) for i in items]
    collected = CollectedItems(items, meta)
    df = collected.to_df()
    inserted_attempt = db.upsert_df("bronze", "raw_items", df)
    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": len(df),
            "insert_attempted": inserted_attempt,
            "collector": meta,
        }
    )
