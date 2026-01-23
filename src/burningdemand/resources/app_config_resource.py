import os
from typing import Optional

from dagster import ConfigurableResource, EnvVar


class AppConfigResource(ConfigurableResource):
    """
    Reads secrets/config from environment variables.
    Missing required vars will cause an immediate validation error on startup.
    """

    # --- GitHub ---
    github_token: str = EnvVar("GITHUB_TOKEN")

    # --- StackExchange ---
    stackexchange_key: Optional[str] = os.getenv("STACKEXCHANGE_KEY")

    # --- Reddit ---
    reddit_client_id: Optional[str] = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret: Optional[str] = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = os.getenv("REDDIT_USER_AGENT", "BurningDemand/0.1")

    # --- Anthropic ---
    anthropic_api_key: str = EnvVar("ANTHROPIC_API_KEY")

    # --- PocketBase ---
    pocketbase_url: str = EnvVar("PB_URL")
    pocketbase_admin_email: str = EnvVar("PB_EMAIL")
    pocketbase_admin_password: str = EnvVar("PB_PASSWORD")
