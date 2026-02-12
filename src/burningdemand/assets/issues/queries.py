"""DB queries for issues asset: group loading and preparation."""

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.assets.groups.representatives import get_cluster_representatives
from burningdemand.utils.config import config


def clear_issues_data_for_date(db: DuckDBResource, date: str) -> None:
    """Remove existing gold.issues and gold.issue_evidence for this partition (enables full recompute on re-materialize)."""
    db.execute("DELETE FROM gold.issue_evidence WHERE group_date = ?", [date])
    db.execute("DELETE FROM gold.issues WHERE group_date = ?", [date])


def get_groups_for_date(db: DuckDBResource, date: str) -> pd.DataFrame:
    """All groups for this date, ranked by authority_score (descending). Used when recomputing labels for the partition."""
    return db.query_df(
        """
        SELECT sg.group_id, sg.group_size, sg.authority_score
        FROM silver.groups sg
        WHERE sg.group_date = ?
        ORDER BY sg.authority_score DESC, sg.group_size DESC
        """,
        [date],
    )


def prepare_groups(
    db: DuckDBResource,
    date: str,
    group_ids: List[int],
) -> Tuple[Dict[int, List[str]], Dict[int, str]]:
    """Load group items from DB and compute representative titles + snippets per group."""
    titles_by_group: Dict[int, List[str]] = {}
    snippets_by_group: Dict[int, str] = {}

    for gid in group_ids:
        group_data = db.query_df(
            """
            SELECT
                e.url_hash,
                e.embedding,
                b.title,
                b.body,
                b.source
            FROM silver.embeddings e
            JOIN bronze.raw_items b ON e.url_hash = b.url_hash
            JOIN silver.group_members gm ON e.url_hash = gm.url_hash
            WHERE gm.group_date = ?
              AND gm.group_id = ?
            """,
            [date, gid],
        )

        if len(group_data) == 0:
            titles_by_group[gid] = []
            snippets_by_group[gid] = ""
            continue

        embeddings_array = np.array(group_data["embedding"].tolist(), dtype=np.float32)
        group_items = pd.DataFrame(
            {
                "title": group_data["title"],
                "body": group_data["body"],
                "source": group_data["source"],
            }
        )
        titles, snippets = get_cluster_representatives(
            group_items,
            embeddings_array,
            max_representatives_count=config.issues.labeling.max_representatives_for_labeling,
            max_snippets_count=config.issues.labeling.max_snippets_for_labeling,
            max_body_length=config.issues.labeling.max_body_length_for_snippet,
        )
        titles_by_group[gid] = titles
        snippets_by_group[gid] = snippets

    return titles_by_group, snippets_by_group
