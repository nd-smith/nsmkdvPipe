"""Custom enrichment handlers for plugin workers."""

from kafka_pipeline.plugins.handlers.delta_writer import DeltaTableWriter, DeltaTableBatchWriter

__all__ = [
    "DeltaTableWriter",
    "DeltaTableBatchWriter",
]
