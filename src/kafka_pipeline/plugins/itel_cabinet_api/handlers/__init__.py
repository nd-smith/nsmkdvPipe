"""iTel Cabinet API plugin handlers."""

from kafka_pipeline.plugins.itel_cabinet_api.handlers.itel_api_sender import (
    ItelApiSender,
    ItelApiBatchSender,
)
from kafka_pipeline.plugins.itel_cabinet_api.handlers.form_parser import ItelFormParser
from kafka_pipeline.plugins.itel_cabinet_api.handlers.dual_table_writer import ItelDualTableWriter
from kafka_pipeline.plugins.itel_cabinet_api.handlers.media_downloader import ItelMediaDownloader

__all__ = [
    "ItelApiSender",
    "ItelApiBatchSender",
    "ItelFormParser",
    "ItelDualTableWriter",
    "ItelMediaDownloader",
]
