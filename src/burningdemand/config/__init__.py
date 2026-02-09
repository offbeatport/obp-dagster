"""
Configuration for the BurningDemand pipeline.

Use the shared config singleton everywhere:

    from burningdemand.config import config

    config.issues.llm.model
    config.embeddings.model
    config.build_system_prompt()
    config.build_label_prompt(titles, size, snippets)
    config.get_core_keywords()
    config.get_source_keywords("github")
    config.matches_keywords(text, "reddit")
"""

from burningdemand.utils.config import Config, config

__all__ = ["Config", "config"]
