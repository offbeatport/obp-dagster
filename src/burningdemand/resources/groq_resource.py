from dagster import ConfigurableResource, EnvVar


class GroqResource(ConfigurableResource):
    """Resource for Groq API access (free tier: 14,400 requests/day)."""

    api_key: str = EnvVar("GROQ_API_KEY")
    model: str = "llama-3.1-70b-versatile"  # Default to best free model
