# Migration Notes: Pipeline Refactoring

## Overview
This refactoring simplifies the pipeline, improves quality/cost, and makes minimal breaking changes to gold outputs.

## Schema Changes

### New Tables
1. **`bronze.raw_items_union`**: Unified view of raw items across all sources
   - Adds `clean_body` column (stripped of boilerplate/quotes/logs/code blocks)
   - Standardizes schema and canonicalizes URLs

2. **`silver.cluster_representatives`**: Top-k central items per cluster
   - Columns: `cluster_date`, `cluster_id`, `url_hash`, `rank`, `distance_to_centroid`

### Modified Tables

1. **`silver.clusters`**:
   - **Removed**: `confidence`, `summary` columns
   - **Added**: `outlier_ratio` (DOUBLE), `mean_distance` (DOUBLE)
   - HDBSCAN allows noise (cluster_id = -1), so clusters may have fewer items

2. **`gold.issues`**:
   - **Added**: `cluster_fingerprint` (VARCHAR), `label_failed` (BOOLEAN, default FALSE)
   - `cluster_fingerprint` enables label reuse across similar clusters
   - `label_failed` marks clusters where LLM labeling failed after retries

## Asset Changes

### New Assets
1. **`bronze.raw_items_union`**: Creates unified, cleaned view of raw items
2. **`silver.cluster_representatives`**: Selects top-k central items per cluster with diversity

### Modified Assets
1. **`silver.embeddings`**: 
   - Now depends on `bronze.raw_items_union` instead of `bronze.raw_items`
   - Embeds only `title + clean_body` (not raw body)

2. **`silver.clusters`**:
   - Replaced MiniBatchKMeans with HDBSCAN
   - Allows noise/unclustered items (cluster_id = -1)
   - Stores quality metrics (outlier_ratio, mean_distance)

3. **`gold.issues`**:
   - Now depends on `silver.cluster_representatives` instead of `silver.summaries`
   - Uses representative titles + short cleaned snippets (not LexRank summaries)
   - Implements label reuse via `cluster_fingerprint`
   - LLM hardening: strict JSON schema, retry once, marks failures
   - Uses local model by default (via `LOCAL_LLM_MODEL` env var), falls back to remote

### Removed Assets
1. **`silver.summaries`**: Deleted (LexRank summarization removed)

## Dependencies

### Added
- `hdbscan`: For density-based clustering

### Removed
- `sumy`: LexRank summarization no longer used

## Breaking Changes

### Minimal Breaking Changes to Gold Outputs
- **`gold.issues` table structure changed**: Added `cluster_fingerprint` and `label_failed` columns
- **Existing gold records**: Will continue to work, but new records will have these fields
- **Migration**: Existing records can be backfilled with `cluster_fingerprint = NULL` and `label_failed = FALSE`

### Data Flow Changes
- **Bronze**: Still per-source (`bronze.raw_items`), but now also has unified view (`bronze.raw_items_union`)
- **Silver**: Embeddings and clustering now run on unified daily union (avoids partial-day clustering)
- **Gold**: Labeling now uses representatives instead of summaries

## Migration Steps

1. **Update schema**: Run the updated `schema/duckdb.sql` to create new tables and modify existing ones
2. **Install dependencies**: Run `uv sync` or `pip install -r requirements.txt` to get `hdbscan`
3. **Backfill existing data** (optional):
   ```sql
   -- Backfill cluster_fingerprint and label_failed for existing gold.issues
   UPDATE gold.issues 
   SET cluster_fingerprint = NULL, label_failed = FALSE 
   WHERE cluster_fingerprint IS NULL;
   ```
4. **Materialize new assets**: Run `bronze.raw_items_union` and `silver.cluster_representatives` for existing dates
5. **Re-run gold.issues**: Will now use representatives and fingerprint-based reuse

## Notes

- **HDBSCAN noise**: Items with `cluster_id = -1` are noise/unclustered and won't be labeled
- **Label reuse**: Clusters with similar representative titles (same fingerprint) will reuse labels, reducing LLM costs
- **LLM fallback**: Large clusters (>50 items) try remote model first; small clusters try local first
- **Text cleaning**: Applied per-source (GitHub: strips code/logs; Reddit/HN: strips quotes/signatures/edit trails)
