import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions, load_assets_from_package_module

# Load .env file from project root
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

# Import the sub-packages
from .assets import bronze, silver, gold

from .resources.llm_resource import LLMResource
from .resources.collectors.collectors_resource import CollectorsResource
from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.pocketbase_resource import PocketBaseResource

# Load assets by layer
# This automatically groups them in the UI and allows for
# clean names like 'items' inside the folders.
bronze_assets = load_assets_from_package_module(
    bronze, group_name="bronze", key_prefix="bronze"
)
silver_assets = load_assets_from_package_module(
    silver, group_name="silver", key_prefix="silver"
)
gold_assets = load_assets_from_package_module(
    gold, group_name="gold", key_prefix="gold"
)

# Combine all assets
all_assets = [*bronze_assets, *silver_assets, *gold_assets]

llm_model = os.getenv("LLM_MODEL", "groq/llama-3.1-70b-versatile")

defs = Definitions(
    assets=all_assets,
    resources={
        "db": DuckDBResource(),
        "embedding": EmbeddingResource(),
        "collector": CollectorsResource(),
        "llm": LLMResource(model=llm_model),
        "pb": PocketBaseResource.from_env(),
    },
)
