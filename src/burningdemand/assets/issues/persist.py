"""Persist labeled issues to gold tables."""

import pandas as pd

from burningdemand.resources.duckdb_resource import DuckDBResource


def save_results(
    db: DuckDBResource,
    results: list[dict],
    failed: list[dict],
    date: str,
) -> None:
    """Persist labeled issues, failed rows, and issue_evidence to gold."""
    if results:
        df = pd.DataFrame(results)
        db.upsert_df(
            "gold",
            "issues",
            df,
            [
                "group_date",
                "group_id",
                "canonical_title",
                "category",
                "desc_problem",
                "desc_current_solutions",
                "desc_impact",
                "desc_details",
                "would_pay_signal",
                "impact_level",
                "group_size",
                "authority_score",
                "label_failed",
            ],
        )

    if failed:
        failed_df = pd.DataFrame(failed)
        db.upsert_df(
            "gold",
            "issues",
            failed_df,
            [
                "group_date",
                "group_id",
                "label_failed",
            ],
        )

    # Backfill evidence for all successfully labeled issues for this date
    # (items that belong to each group), so gold.issue_evidence stays in sync.
    if results or failed:
        db.execute(
            """
            INSERT INTO gold.issue_evidence (group_date, group_id, url_hash, source, url, title, body, posted_at)
            SELECT gm.group_date,
                   gm.group_id,
                   gm.url_hash,
                   b.source,
                   b.url,
                   b.title,
                   b.body,
                   b.created_at
            FROM silver.group_members gm
            JOIN bronze.raw_items b
              ON gm.url_hash = b.url_hash
            WHERE gm.group_date = ?
              AND EXISTS (
                  SELECT 1 FROM gold.issues gi
                  WHERE gi.group_date = gm.group_date
                    AND gi.group_id = gm.group_id
                    AND gi.label_failed = FALSE
              )
            ON CONFLICT DO NOTHING
            """,
            [date],
        )
