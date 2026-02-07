import os
from pathlib import Path

from dotenv import load_dotenv
from dagster import Definitions, EnvVar

# Load .env file from project root (so config.py env defaults are overridable)
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

from burningdemand.utils.config import config

# Import assets directly
from .assets import (
    raw_gh_issues,
    raw_gh_discussions,
    raw_rd,
    raw_so,
    raw_hn,
    embeddings,
    clusters,
    issues,
    live_issues,
    live_evidence,
)
from .resources.collectors.collectors_resource import CollectorsResource
from .resources.collectors.github_collector import GitHubCollector
from .resources.collectors.hackernews_collector import HackerNewsCollector
from .resources.collectors.reddit_collector import RedditCollector
from .resources.collectors.stackoverflow_collector import StackOverflowCollector
from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.pocketbase_resource import PocketBaseResource

# Combine all assets
# Assets use AssetKey with prefixes (bronze, silver, gold) in their dependencies
# Dagster will automatically handle the key prefixes based on the AssetKey references
all_assets = [
    raw_gh_issues,
    raw_gh_discussions,
    raw_rd,
    raw_so,
    raw_hn,
    embeddings,
    clusters,
    issues,
    live_issues,
    live_evidence,
]

defs = Definitions(
    assets=all_assets,
    resources={
        "db": DuckDBResource(),
        "embedding": EmbeddingResource(model_name=config.embeddings.model),
        "collector": CollectorsResource(
            github_collector=GitHubCollector(github_token=EnvVar("GITHUB_TOKEN")),
            stackoverflow_collector=StackOverflowCollector(
                stackexchange_key=os.getenv("STACKEXCHANGE_KEY"),
            ),
            reddit_collector=RedditCollector(
                reddit_client_id=os.getenv("REDDIT_CLIENT_ID"),
                reddit_client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            ),
            hackernews_collector=HackerNewsCollector(),
        ),
        "pb": PocketBaseResource.from_env(),
    },
)
