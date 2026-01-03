"""
Event message schemas - moved to xact domain.

This module re-exports from kafka_pipeline.xact.schemas.events.
"""

from kafka_pipeline.xact.schemas.events import EventMessage

__all__ = ["EventMessage"]
