"""
This module is the entry point for the library.

The library exposes helpers to create custom sources for
tradesignals stream processing applications.
"""
from csv_source import CSVSource
from http_source import HttpSource
from quix import create_app
from websocket_source import WebSocketSource

__all__ = [
    "create_app",
    "WebSocketSource",
    "HttpSource",
    "CSVSource",
]
