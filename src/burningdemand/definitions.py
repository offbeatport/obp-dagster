import os
from pathlib import Path

from dotenv import load_dotenv
from dagster import Definitions, EnvVar

# Load .env file from project root (so config.py env defaults are overridable)
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

# Import assets directly
from .assets import (
    raw_gh_issues,
    raw_gh_discussions,
    raw_gh_pull_requests,
    embeddings,
    clusters,
    issues,
    live_issues,
    live_evidence,
)
from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.github_resource import GitHubResource
from .resources.hackernews_resource import HackerNewsResource
from .resources.pocketbase_resource import PocketBaseResource
from .resources.reddit_resource import RedditResource
from .resources.stackoverflow_resource import StackOverflowResource

# Combine all assets
# Assets use AssetKey with prefixes (bronze, silver, gold) in their dependencies
# Dagster will automatically handle the key prefixes based on the AssetKey references
all_assets = [
    raw_gh_issues,
    raw_gh_discussions,
    raw_gh_pull_requests,
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
        "embedding": EmbeddingResource(),
        "github": GitHubResource(github_token=EnvVar("GITHUB_TOKEN")),
        "reddit": RedditResource(
            reddit_client_id=os.getenv("REDDIT_CLIENT_ID"),
            reddit_client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        ),
        "stackoverflow": StackOverflowResource(
            stackexchange_key=os.getenv("STACKEXCHANGE_KEY"),
        ),
        "hackernews": HackerNewsResource(),
        "pb": PocketBaseResource.from_env(),
    },
)
