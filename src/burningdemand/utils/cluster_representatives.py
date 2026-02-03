# burningdemand_dagster/utils/cluster_representatives.py
"""Utility functions for computing cluster representatives on-the-fly."""
from pprint import pprint
import pandas as pd
import numpy as np
from typing import List, Tuple
from burningdemand.utils.text_cleaning import clean_body


def normalize_title(title: str) -> str:
    """Normalize title for similarity comparison."""
    return title.lower().strip()


def titles_too_similar(title1: str, title2: str, threshold: float = 0.8) -> bool:
    """Check if two titles are too similar using simple word overlap."""
    words1 = set(normalize_title(title1).split())
    words2 = set(normalize_title(title2).split())
    if not words1 or not words2:
        return False
    intersection = words1 & words2
    union = words1 | words2
    jaccard = len(intersection) / len(union) if union else 0.0
    return jaccard >= threshold


def select_representatives(
    items_df: pd.DataFrame,
    embeddings_array: np.ndarray,
    k: int = 5,
    diversity_threshold: float = 0.8,
) -> pd.DataFrame:
    """
    Select top-k central items (medoid/closest-to-centroid) with diversity enforcement.

    Args:
        items_df: DataFrame with items (must have 'title' column)
        embeddings_array: numpy array of shape (n_items, embedding_dim)
        k: Number of representatives to select
        diversity_threshold: Jaccard similarity threshold for title diversity

    Returns:
        DataFrame with selected representatives (subset of items_df with added 'rank' column)
    """
    if len(items_df) == 0 or len(embeddings_array) == 0:
        return pd.DataFrame()

    # Compute centroid
    centroid = np.mean(embeddings_array, axis=0)

    # Compute distances from each point to centroid
    distances = np.linalg.norm(embeddings_array - centroid, axis=1)

    # Sort by distance (closest first)
    sorted_indices = np.argsort(distances)

    # Select representatives with diversity
    selected_indices = []
    selected_titles = []

    for idx in sorted_indices:
        if len(selected_indices) >= k:
            break

        title = str(items_df.iloc[idx].get("title", "") or "")
        normalized_title = normalize_title(title)

        # Check diversity: avoid near-duplicate titles
        is_diverse = True
        for selected_title in selected_titles:
            if titles_too_similar(title, selected_title, diversity_threshold):
                is_diverse = False
                break

        if is_diverse:
            selected_indices.append(int(idx))
            selected_titles.append(normalized_title)

    # Return selected items with their rank
    representatives = items_df.iloc[selected_indices].copy()
    representatives["rank"] = range(1, len(representatives) + 1)

    return representatives


def get_cluster_representatives(
    items_df: pd.DataFrame,
    embeddings_array: np.ndarray,
    max_representatives_count: int = 10,
    max_snippets_count: int = 5,
    max_body_length: int = 500,
) -> Tuple[List[str], str]:
    """
    Get representative titles and snippets for a cluster.

    Args:
        items_df: DataFrame with cluster items (must have 'title', 'body', 'source' columns)
        embeddings_array: numpy array of shape (n_items, embedding_dim)
        max_representatives_count: Number of representatives to select
        max_snippets_count: Number of snippets to include
        max_body_length: Maximum length of the body to include in the snippet
    Returns:
        Tuple of (list of titles, combined snippets string)
    """
    representatives = select_representatives(
        items_df, embeddings_array, k=max_representatives_count
    )

    if len(representatives) == 0:
        return [], ""
    titles = representatives["title"].fillna("").astype(str).tolist()

    snippets = []
    for _, row in representatives.iterrows():
        body = str(row.get("body", "") or "")
        source = str(row.get("source", "") or "github")
        clean_body_text = clean_body(body, source)
        snippet = clean_body_text[:max_body_length].strip()
        if snippet:
            snippets.append(snippet)

    snippets_str = " | ".join(snippets[:max_snippets_count])

    return titles, snippets_str
