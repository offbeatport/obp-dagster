# burningdemand_dagster/resources/embedding_resource.py
from typing import List, Optional

import numpy as np
from dagster import ConfigurableResource
from pydantic import Field


class EmbeddingResource(ConfigurableResource):
    """Model name read from config at setup time if not provided (avoids import-time config)."""

    model_name: Optional[str] = Field(default=None)

    def setup_for_execution(self, context) -> None:
        from sentence_transformers import SentenceTransformer

        from burningdemand.utils.config import config

        name = self.model_name or config.embeddings.model
        self._model = SentenceTransformer(name)

    def encode(self, texts: List[str]) -> np.ndarray:
        from burningdemand.utils.config import config

        return self._model.encode(
            texts,
            batch_size=config.embeddings.encode_batch_size,
            show_progress_bar=True,
            normalize_embeddings=True,
        )
