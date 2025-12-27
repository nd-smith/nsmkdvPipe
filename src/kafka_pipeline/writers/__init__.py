"""
Delta Lake writers for Kafka pipeline.

Provides specialized writers for:
- Event analytics (xact_events table)
- Download inventory (xact_attachments table)
"""

from kafka_pipeline.writers.delta_events import DeltaEventsWriter
from kafka_pipeline.writers.delta_inventory import DeltaInventoryWriter

__all__ = ["DeltaEventsWriter", "DeltaInventoryWriter"]
