# Dagster Pipeline: Issue Collection and Clustering

## Overview
A data pipeline that collects issues from external sources (GitHub, StackOverflow, Reddit, HackerNews), clusters them by semantic similarity, and generates structured labels using LLMs. Uses on-the-fly computation for embeddings and representatives to minimize storage.

## Pipeline Flow
```
bronze.raw_items → silver.clusters → gold.issues → gold.live_issues
```

## Assets

### BRONZE LAYER

#### `bronze.raw_items`
- **Partition**: `source_day_partitions` (multi-partition: source × date)
- **Purpose**: Collect raw issues from external APIs
- **Sources**: github, stackoverflow, reddit, hackernews
- **Process**:
  - Fetches items via `CollectorsResource` (handles rate limiting per API)
  - Normalizes URLs and computes `url_hash` (SHA256 of normalized URL)
  - Stores: `url_hash`, `source`, `collection_date`, `url`, `title`, `body`, `created_at`, `comment_count`, `vote_count`
- **Storage**: `bronze.raw_items` table

---

### SILVER LAYER

#### `silver.clusters`
- **Partition**: `daily_partitions` (daily)
- **Dependencies**: `bronze.raw_items`
- **Purpose**: Cluster items by semantic similarity using HDBSCAN
- **Process**:
  1. Reads items from `bronze.raw_items` for the date
  2. **Computes embeddings on-the-fly** using `compute_embeddings_for_items()`:
     - Cleans body text per source (GitHub: strips code/logs; Reddit/HN: strips quotes/signatures/edit trails)
     - Combines `title + clean_body`
     - Generates 384-dim embeddings via sentence-transformers (all-MiniLM-L6-v2)
  3. Runs HDBSCAN clustering:
     - `min_cluster_size = max(5, n_items // 200)`
     - `min_samples = min_cluster_size // 2`
     - Allows noise (cluster_id = -1 for unclustered items)
  4. Computes quality metrics per cluster:
     - `outlier_ratio`: fraction of noise points
     - `mean_distance`: mean distance from points to cluster centroid
  5. Stores cluster metadata and assignments
- **Storage**:
  - `silver.clusters`: cluster metadata (cluster_date, cluster_id, cluster_size, outlier_ratio, mean_distance)
  - `silver.cluster_assignments`: url_hash → cluster_id mapping

---

### GOLD LAYER

#### `gold.issues`
- **Partition**: `daily_partitions` (daily)
- **Dependencies**: `silver.clusters`
- **Purpose**: Label clusters using LLM to extract structured issue information
- **Process**:
  1. Gets unlabeled clusters from `silver.clusters`
  2. For each cluster, **computes representatives on-the-fly**:
     - Reads cluster items from `bronze.raw_items` + `silver.cluster_assignments`
     - Computes embeddings on-the-fly
     - Selects top-5 central items (closest to centroid) with diversity enforcement (avoids near-duplicate titles)
     - Extracts titles and short cleaned snippets (first 200 chars of clean_body)
  3. **Label reuse via fingerprint**:
     - Computes `cluster_fingerprint = SHA256(sorted(normalized_rep_titles[:5]))`
     - If fingerprint exists in `gold.issues`, reuses existing label (skips LLM call)
  4. **LLM labeling** (if no reuse):
     - Uses LiteLLM with strict JSON schema enforcement
     - Retries once on validation failure
     - Falls back to remote model if local model fails (or for large clusters >50 items)
     - Marks `label_failed = TRUE` if all attempts fail
  5. Extracts structured fields:
     - `canonical_title`: Generic problem description (max 120 chars)
     - `category`: ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other
     - `description`: Root problem in 1-2 sentences
     - `would_pay_signal`: Boolean
     - `impact_level`: low|medium|high
- **Storage**:
  - `gold.issues`: Labeled cluster records
  - `gold.issue_evidence`: Denormalized evidence (all items in each labeled cluster)

#### `gold.live_issues`
- **Partition**: `daily_partitions` (daily)
- **Dependencies**: `gold.issues`
- **Purpose**: Sync labeled issues to PocketBase for live access
- **Process**: Creates/updates issue records in external system

---

## Key Technical Details

### Text Cleaning (`utils/text_cleaning.py`)
- **GitHub**: Strips code blocks (```...```), inline code (`...`), log patterns (timestamps, error traces)
- **Reddit/HN**: Removes quoted blocks (>), signatures (---, --), "edit:" trails, email patterns

### Embeddings (`utils/embeddings.py`)
- Model: `all-MiniLM-L6-v2` (384 dimensions)
- Input: `title + clean_body` (cleaned per source)
- Batch size: 1000 items
- **Computed on-the-fly** (not stored)

### Clustering
- Algorithm: HDBSCAN (density-based, allows noise)
- Metric: Euclidean distance
- Cluster selection: EOM (Excess of Mass)
- Noise handling: Items with `cluster_id = -1` are unclustered

### Representatives (`utils/cluster_representatives.py`)
- Selection: Top-k items closest to cluster centroid
- Diversity: Jaccard similarity threshold (0.8) to avoid near-duplicate titles
- **Computed on-the-fly** (not stored)

### Label Reuse
- Fingerprint: `SHA256(sorted(normalized_rep_titles[:5]))`
- Purpose: Reduce LLM costs by reusing labels for similar clusters

### LLM Hardening
- Strict JSON schema: `response_format={"type": "json_object"}`
- Pydantic validation: `IssueLabel.model_validate()`
- Retry: Once on validation failure
- Fallback: Local model → Remote model (Groq/Anthropic)
- Failure handling: Marks `label_failed = TRUE` and continues

---

## Data Storage

### Bronze
- `bronze.raw_items`: Raw collected items (per source, per date)

### Silver
- `silver.clusters`: Cluster metadata only
- `silver.cluster_assignments`: url_hash → cluster_id mapping

### Gold
- `gold.issues`: Labeled cluster records
- `gold.issue_evidence`: Denormalized evidence (items per cluster)
- `gold.live_issues`: PocketBase sync tracking

**Note**: Embeddings and representatives are **not stored** - computed on-the-fly when needed.

---

## Partitioning Strategy

- **Bronze**: `source_day_partitions` (multi-partition: source × date)
  - Allows per-source collection with different schedules/rates
- **Silver/Gold**: `daily_partitions` (daily)
  - Clustering and labeling operate on unified daily data

---

## Resources

- `CollectorsResource`: Handles API collection with rate limiting (per-domain configurable)
- `EmbeddingResource`: Sentence transformers model (all-MiniLM-L6-v2)
- `LLMResource`: LiteLLM wrapper (supports 100+ providers)
- `DuckDBResource`: DuckDB database operations
- `PocketBaseResource`: External system sync
