"""iTel Cabinet API Plugin."""

from kafka_pipeline.plugins.itel_cabinet_api.handlers.itel_api_sender import (
    ItelApiSender,
    ItelApiBatchSender,
)

__all__ = [
    "ItelApiSender",
    "ItelApiBatchSender",
]
