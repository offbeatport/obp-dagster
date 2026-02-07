# Dagster Assets Pipeline Summary

## Overview
This is a data pipeline that collects issues from external sources (GitHub, StackOverflow, Reddit, HackerNews), processes them through semantic analysis and clustering, and generates structured issue labels using LLMs.

## Pipeline Flow
```
bronze.raw_items → silver.embeddings → silver.clusters → silver.summaries → gold.issues → gold.live_issues
```

## Asset Details

### BRONZE LAYER

#### `bronze.raw_items`
- **Purpose**: Collect raw issues from external sources
- **Partition**: `source_day_partitions` (partitioned by source and date)
- **Dependencies**: None (entry point)
- **What it does**:
  - Collects issues from GitHub, StackOverflow, Reddit, or HackerNews using resources
  - Normalizes URLs and generates URL hashes
  - Stores raw data: url_hash, source, collection_date, url, title, body, created_at
  - Handles rate limiting per API (e.g., 30 requests per 60s for GitHub)
- **Output**: `bronze.raw_items` table

---

### SILVER LAYER

#### `silver.embeddings`
- **Purpose**: Generate vector embeddings for semantic similarity
- **Partition**: `daily_partitions`
- **Dependencies**: `bronze.raw_items`
- **What it does**:
  - Takes title + body text from raw items
  - Generates 384-dimensional embeddings using sentence transformers (all-MiniLM-L6-v2)
  - Processes in batches of 1000 items
  - Stores: url_hash, embedding (FLOAT[384]), embedding_date, cluster_date (NULL), cluster_id (NULL)
- **Output**: `silver.items` table (with embeddings, cluster fields initially NULL)

#### `silver.clusters`
- **Purpose**: Group similar items into clusters
- **Partition**: `daily_partitions`
- **Dependencies**: `silver.embeddings`
- **What it does**:
  - Takes items with embeddings but no cluster assignment
  - Runs MiniBatchKMeans clustering (n_clusters = max(10, items/100))
  - Updates `silver.items` with cluster_date and cluster_id
  - Creates cluster metadata in `silver.clusters`: cluster_date, cluster_id, cluster_size, summary (NULL initially)
- **Output**: 
  - Updates `silver.items` with cluster assignments
  - Creates records in `silver.clusters` table

#### `silver.summaries`
- **Purpose**: Generate text summaries for each cluster
- **Partition**: `daily_partitions`
- **Dependencies**: `silver.clusters`
- **What it does**:
  - Gets clusters without summaries
  - For each cluster, samples up to 10 items and combines their bodies
  - Uses LexRank algorithm (extractive summarization) to generate 3-sentence summaries
  - Updates `silver.clusters.summary` field
- **Output**: Updates `silver.clusters` table with summaries

---

### GOLD LAYER

#### `gold.issues`
- **Purpose**: Label clusters using LLM to create structured issue records
- **Partition**: `daily_partitions`
- **Dependencies**: `silver.summaries`
- **What it does**:
  - Gets unlabeled clusters (clusters without gold.issues records)
  - For each cluster, prepares:
    - Up to 10 sample titles
    - Cluster summary (from silver.clusters)
  - Sends to LLM (via LiteLLM) with prompt to extract:
    - canonical_title (max 120 chars)
    - category (ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other)
    - description (1-2 sentences)
    - would_pay_signal (boolean)
    - impact_level (low|medium|high)
  - Processes up to 8 clusters concurrently (semaphore)
  - Saves to `gold.issues` table
  - Denormalizes evidence: creates `gold.issue_evidence` records linking all items in cluster to the issue
- **Output**: 
  - `gold.issues` table (structured issue records)
  - `gold.issue_evidence` table (links items to issues)

#### `gold.live_issues`
- **Purpose**: Sync labeled issues to PocketBase for live access
- **Partition**: `daily_partitions`
- **Dependencies**: `gold.issues`
- **What it does**:
  - Gets issues that haven't been synced to PocketBase
  - For each issue:
    - Checks if issue exists in PocketBase (by cluster_date + cluster_id)
    - Creates issue if missing
    - Creates evidence records for all items in the cluster
  - Tracks sync status in `gold.live_issues` table
- **Output**: Creates/updates records in PocketBase

---

## Database Schema

### Bronze Tables
- `bronze.raw_items`: Raw collected items (url_hash PK, source, collection_date, url, title, body, created_at)

### Silver Tables
- `silver.items`: Items with embeddings and cluster assignments (url_hash PK, embedding FLOAT[384], embedding_date, cluster_date, cluster_id)
- `silver.clusters`: Cluster metadata (cluster_date + cluster_id PK, cluster_size, confidence, summary)

### Gold Tables
- `gold.issues`: Labeled issues (cluster_date + cluster_id PK, canonical_title, category, description, would_pay_signal, impact_level, cluster_size)
- `gold.issue_evidence`: Evidence linking items to issues (cluster_date + cluster_id + url_hash PK, source, url, title, body, posted_at)
- `gold.live_issues`: Sync tracking (cluster_date + cluster_id PK, issue_id, synced_at)

---

## Key Technologies
- **Embeddings**: sentence-transformers (all-MiniLM-L6-v2), 384-dimensional vectors
- **Clustering**: scikit-learn MiniBatchKMeans
- **Summarization**: sumy library with LexRank algorithm
- **LLM**: LiteLLM (supports Groq, Anthropic, OpenAI)
- **Database**: DuckDB
- **External Sync**: PocketBase

---

## Rate Limiting
- GitHub: 30 requests per 60 seconds
- StackExchange: 30 requests per 60 seconds
- Reddit: 60 requests per 60 seconds
- HackerNews: 100 requests per 60 seconds

Rate limiting is enforced with strict batching: send N requests, wait 60 seconds, send next N requests.
