"""
This module is the entry point for the library.

The library exposes helpers to create custom sources for
tradesignals stream processing applications.
"""
from common.http_source import HttpSource
from common.quix import create_app
from common.websocket_source import WebSocketSource

from .csv_source import CSVSource

__all__ = [
    "create_app",
    "WebSocketSource",
    "HttpSource",
    "CSVSource",
]
