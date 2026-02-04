"""
Central configuration for the BurningDemand pipeline.
Override defaults via environment variables where noted.
"""

import os

# -----------------------------------------------------------------------------
# LLM (cluster labeling)
# -----------------------------------------------------------------------------
# Override: set LLM_MODEL in env (e.g. "ollama/gemma3:4b", "groq/llama-3.1-70b-versatile")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-5-mini")
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_MAX_TOKENS = 2000
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://llm.offbeatport.com")

# -----------------------------------------------------------------------------
# Labeling context (how much cluster content we send to the LLM)
# -----------------------------------------------------------------------------
MAX_REPRESENTATIVES_FOR_LABELING = 10
MAX_SNIPPETS_FOR_LABELING = 5
MAX_BODY_LENGTH_FOR_SNIPPET = 500

# -----------------------------------------------------------------------------
# Issues asset (concurrent LLM calls per partition)
# -----------------------------------------------------------------------------
LLM_CONCURRENCY_PER_ISSUES_PARTITION = 8

# -----------------------------------------------------------------------------
# Embeddings
# -----------------------------------------------------------------------------
# Override: set EMBEDDING_MODEL in env (e.g. "all-MiniLM-L6-v2", "BAAI/bge-small-en-v1.5")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
# Batch size when calling the embedding model (SentenceTransformer.encode)
EMBEDDING_ENCODE_BATCH_SIZE = 256
# Batch size for splitting items in the embeddings asset (chunks of items to embed per run)
EMBEDDINGS_ASSET_BATCH_SIZE = 1000

# -----------------------------------------------------------------------------
# Clustering (HDBSCAN + rolling window)
# -----------------------------------------------------------------------------
# Number of days of embeddings to include when clustering (window_end = partition date)
CLUSTERING_ROLLING_WINDOW_DAYS = 7
# min_cluster_size = max(CLUSTERING_MIN_CLUSTER_SIZE_FLOOR, n // CLUSTERING_MIN_CLUSTER_SIZE_DIVISOR)
CLUSTERING_MIN_CLUSTER_SIZE_FLOOR = 5
CLUSTERING_MIN_CLUSTER_SIZE_DIVISOR = 200
# min_samples = max(CLUSTERING_MIN_SAMPLES_FLOOR, min_cluster_size // CLUSTERING_MIN_SAMPLES_DIVISOR)
CLUSTERING_MIN_SAMPLES_FLOOR = 3
CLUSTERING_MIN_SAMPLES_DIVISOR = 2
CLUSTERING_METRIC = "euclidean"
CLUSTERING_SELECTION_METHOD = "eom"

# -----------------------------------------------------------------------------
# Cluster representatives (diversity when selecting titles/snippets)
# -----------------------------------------------------------------------------
# Jaccard similarity threshold for title diversity (0.8 = titles must be fairly different)
REPRESENTATIVES_DIVERSITY_THRESHOLD = 0.8
