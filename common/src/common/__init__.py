"""
This module is the entry point for the library.

The library exposes helpers to create custom sources for
tradesignals stream processing applications.
"""
from .csv_source import BaseCsvSource
from .http_source import BaseHttpSource
from .quix import create_app
from .websocket_source import BaseWebSocketSource

__all__ = [
    "create_app",
    "BaseWebSocketSource",
    "BaseHttpSource",
    "BaseCsvSource",
]
