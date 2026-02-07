# burningdemand_dagster/resources/embedding_resource.py
from typing import List

import numpy as np
from dagster import ConfigurableResource
from pydantic import Field

from burningdemand.utils.config import config


class EmbeddingResource(ConfigurableResource):
    model_name: str = Field(default="")

    def setup_for_execution(self, context) -> None:
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(self.model_name or config.embeddings.model)

    def encode(self, texts: List[str]) -> np.ndarray:
        return self._model.encode(
            texts,
            batch_size=config.embeddings.encode_batch_size,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
