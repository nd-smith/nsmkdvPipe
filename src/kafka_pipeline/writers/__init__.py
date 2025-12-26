"""
Delta Lake writers for Kafka pipeline.

Provides specialized writers for:
- Event analytics (xact_events table)
- Download inventory (xact_attachments table)
"""

from kafka_pipeline.writers.delta_events import DeltaEventsWriter

__all__ = ["DeltaEventsWriter"]
