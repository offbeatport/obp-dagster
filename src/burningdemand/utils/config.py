"""
Unified configuration for BurningDemand.

Loaded from YAML into validated models.
If required keys are missing or types are wrong, import will fail fast.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from burningdemand.utils.llm_schema import CATEGORIES


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}, got {type(data)}")
    return data


class LlmConfig(BaseModel):
    model: str
    base_url: str
    max_tokens: int
    api_key: str | None = None


class LabelingConfig(BaseModel):
    max_representatives_for_labeling: int
    max_snippets_for_labeling: int
    max_body_length_for_snippet: int


class IssuesConfig(BaseModel):
    llm_concurrency_per_issues_partition: int
    labeling: LabelingConfig
    llm: LlmConfig


class EmbeddingConfig(BaseModel):
    model: str
    encode_batch_size: int
    asset_batch_size: int


class RepresentativesConfig(BaseModel):
    diversity_threshold: float


class ClusteringConfig(BaseModel):
    rolling_window_days: int
    min_cluster_size_floor: int
    min_cluster_size_divisor: int
    min_samples_floor: int
    min_samples_divisor: int
    metric: str
    selection_method: str
    representatives: RepresentativesConfig


class RawGhIssuesAssetConfig(BaseModel):
    queries_per_day: int
    max_parallel: int
    min_reactions: int
    min_comments: int
    max_comments: int
    max_labels: int
    per_page: int


class RawGhDiscussionsAssetConfig(BaseModel):
    queries_per_day: int
    max_parallel: int = 2
    min_comments: int
    max_comments: int
    max_labels: int
    per_page: int


class RawGhPullRequestsAssetConfig(BaseModel):
    queries_per_day: int
    max_parallel: int = 2
    min_comments: int
    max_comments: int
    max_labels: int
    per_page: int


class Config(BaseModel):
    # Forbid unknown keys in scalar config; safer during refactors.
    model_config = ConfigDict(extra="forbid")

    # Root-level keys matching config.yaml (asset names + section names)
    raw_gh_issues: RawGhIssuesAssetConfig
    raw_gh_discussions: RawGhDiscussionsAssetConfig
    raw_gh_pull_requests: RawGhPullRequestsAssetConfig
    issues: IssuesConfig
    embeddings: EmbeddingConfig
    clustering: ClusteringConfig
    keywords: dict[str, Any]
    prompts: dict[str, Any]

    def _source_section(self, source: str | None) -> dict[str, Any]:
        if not source:
            return {}
        sources = self.keywords.get("sources") or {}
        section = (
            (sources.get(source) if isinstance(sources, dict) else None)
            or self.keywords.get(source)
            or {}
        )
        return section if isinstance(section, dict) else {}

    def get_core_keywords(self) -> list[str]:
        core = self.keywords.get("core")
        if core is not None:
            return list(core)
        # Fallback to keywords["all"]["keywords"]; normal KeyError if missing.
        return list(self.keywords["all"]["keywords"])

    def get_source_keywords(self, source: str | None) -> list[str]:
        core = self.get_core_keywords()
        section = self._source_section(source)
        extra = section.get("extra") or section.get("extra_keywords") or []
        return core + list(extra)

    def get_source_tags(self, source: str) -> list[str]:
        tags = self._source_section(source).get("tags") or []
        return list(tags)

    def build_stackoverflow_tags(self) -> list[str]:
        return self.get_source_tags("stackoverflow")

    def matches_keywords(self, text: str, source: str | None = None) -> bool:
        if not text:
            return False
        text_lower = text.lower()
        keywords = (
            self.get_source_keywords(source) if source else self.get_core_keywords()
        )
        return any(kw.lower() in text_lower for kw in keywords)

    def get_reddit_subreddits(self) -> list[str]:
        # Required; normal KeyError if missing.
        return list(self.keywords["reddit"]["subreddits"])

    def build_system_prompt(self) -> str:
        raw = self.prompts["system_prompt"]
        return str(raw).format(CATEGORIES=", ".join(CATEGORIES))

    def build_system_prompt_lite(self) -> str:
        raw = self.prompts["system_prompt_lite"]
        return str(raw)

    def build_label_prompt(self, titles: list[str], size: int, snippets: str) -> str:
        titles_str = "\n".join(f"- {t}" for t in titles if t)
        snippets_section = f"\n\nSnippets:\n{snippets}" if snippets else ""
        return (
            "# PROBLEM DATA TO ANALYZE:\n\n"
            f"Number of issues: {len(titles)} (sampled from a cluster of {size}):\n\n"
            f"Titles:\n{titles_str}"
            f"{snippets_section}\n"
        )


def load_config(config_dir: Path | None = None) -> Config:
    config_dir = config_dir or Path(__file__).resolve().parent.parent / "config"

    scalar = _read_yaml(config_dir / "config.yaml")
    data: dict[str, Any] = {
        **scalar,
        "keywords": _read_yaml(config_dir / "keywords.yaml"),
        "prompts": _read_yaml(config_dir / "prompts.yaml"),
    }

    # Let Pydantic surface any validation errors directly.
    return Config.model_validate(data)


config = load_config()

__all__ = [
    "Config",
    "LlmConfig",
    "LabelingConfig",
    "IssuesConfig",
    "EmbeddingConfig",
    "ClusteringConfig",
    "RepresentativesConfig",
    "RawGhDiscussionsAssetConfig",
    "RawGhIssuesAssetConfig",
    "RawGhPullRequestsAssetConfig",
    "RawGhPrReviewsAssetConfig",
    "RawGhRepositoriesAssetConfig",
    "load_config",
    "config",
]
