import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions, EnvVar

# Load .env file from project root
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

# Import assets directly
from .assets import raw_items, embeddings, clusters, issues, live_issues

from .resources.collectors.collectors_resource import CollectorsResource
from .resources.collectors.github_collector import GitHubCollector
from .resources.collectors.stackoverflow_collector import StackOverflowCollector
from .resources.collectors.reddit_collector import RedditCollector
from .resources.collectors.hackernews_collector import HackerNewsCollector
from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.pocketbase_resource import PocketBaseResource

# Combine all assets
# Assets use AssetKey with prefixes (bronze, silver, gold) in their dependencies
# Dagster will automatically handle the key prefixes based on the AssetKey references
all_assets = [
    raw_items,
    embeddings,
    clusters,
    issues,
    live_issues,
]

defs = Definitions(
    assets=all_assets,
    resources={
        "db": DuckDBResource(),
        "embedding": EmbeddingResource(),
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
