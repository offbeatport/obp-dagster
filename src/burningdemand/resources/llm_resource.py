import os
from typing import Optional
from dagster import ConfigurableResource, EnvVar


class LLMResource(ConfigurableResource):
    """Unified LLM resource using LiteLLM (supports 100+ providers)."""

    # Model string format: "provider/model-name" or just "model-name" for OpenAI
    # Examples: "groq/llama-3.1-70b-versatile", "anthropic/claude-sonnet-4-20250514", "gpt-4"
    model: str = "groq/llama-3.1-70b-versatile"  # Default to free Groq model
    
    # API keys - set the ones you need based on your model choice
    # Groq (free tier: 14,400 req/day) - required for default model
    groq_api_key: str = EnvVar("GROQ_API_KEY")
    # Anthropic (paid) - optional
    anthropic_api_key: Optional[str] = os.getenv("ANTHROPIC_API_KEY")
    # OpenAI (if using OpenAI models) - optional
    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
