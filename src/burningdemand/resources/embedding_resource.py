# burningdemand_dagster/resources/embedding_resource.py
from typing import List

import numpy as np
from dagster import ConfigurableResource
from pydantic import Field

from burningdemand.config import EMBEDDING_ENCODE_BATCH_SIZE, EMBEDDING_MODEL


class EmbeddingResource(ConfigurableResource):
    model_name: str = Field(default=EMBEDDING_MODEL)

    def setup_for_execution(self, context) -> None:
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(self.model_name)

    def encode(self, texts: List[str]) -> np.ndarray:
        return self._model.encode(
            texts, batch_size=EMBEDDING_ENCODE_BATCH_SIZE, show_progress_bar=False
        )
