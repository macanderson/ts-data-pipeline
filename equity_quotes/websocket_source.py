from asyncio.log import logger
import json
import logging
import sys
import threading
import time
from typing import Callable, Optional

from quixstreams.sources.base.source import BaseSource

import websocket


# from quixstreams.models import Row, RowProducer, Topic
logger = logging.getLogger(__name__)
# Configure logger to pretty print
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class BaseWebSocketSource(BaseSource):
    """
    A WebSocket-based implementation of the BaseSource class.

    This source connects to a WebSocket URL and streams data to a specified topic.
    """

    def __init__(
        self,
        topic: str,
        ws_url: str,
        on_message: Callable = None,
        key_serializer: Callable = str,
        key_deserializer: Callable = str,
        value_serializer: Callable = json.dumps,
        value_deserializer: Callable = json.loads,
        reconnect_delay: int = 5,
        on_message_processed: Optional[Callable] = None,
        on_producer_error: Optional[Callable] = None
    ):
        """
        Initialize the WebSocket source with a topic, WebSocket URL, and serialization options.

        :param topic: The name of the topic and identifier for the source.
        :param ws_url: The WebSocket URL to connect to.
        :param on_message: Callback for handling incoming WebSocket messages.
        :param key_serializer: Serializer for message keys (default is str conversion).
        :param key_deserializer: Deserializer for message keys (default is str conversion).
        :param value_serializer: Serializer for outgoing message values (default is JSON).
        :param value_deserializer: Deserializer for incoming message values (default is JSON).
        :param reconnect_delay: Delay in seconds before attempting to reconnect on failure.
        :param on_message_processed: Callback triggered after a message is successfully processed.
        :param on_producer_error: Callback triggered if an error occurs during message production.
        """
        self.topic = topic
        self.ws_url = ws_url
        self._running = False
        self._ws = None
        self._thread = None
        self.on_message = on_message if on_message else self._default_on_message
        self.key_serializer = key_serializer
        self.key_deserializer = key_deserializer
        self.value_serializer = value_serializer
        self.value_deserializer = value_deserializer
        self.reconnect_delay = reconnect_delay
        self.on_message_processed = on_message_processed
        self.on_producer_error = on_producer_error

    def start(self):
        """
        Starts the WebSocket connection in a separate thread with reconnection handling.
        """
        self._running = True
        self._start_websocket()

    def _start_websocket(self):
        """
        Initializes and runs the WebSocket connection with automatic reconnection.
        """
        while self._running:
            try:
                logger.info(f"Attempting to start WebSocket source for topic '{self.topic}'...")
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self.on_message,
                    on_close=self._on_close,
                    on_open=self._on_open,
                    on_error=self._on_error
                )
                self._thread = threading.Thread(target=self._ws.run_forever)
                self._thread.start()
                self._thread.join()  # Wait for thread to finish (closes if _ws.close is called)
            except Exception as e:
                logger.error(f"Error in WebSocket connection for topic '{self.topic}': {e}")

            if self._running:
                logger.info(f"Reconnecting WebSocket for topic '{self.topic}' after {self.reconnect_delay} seconds...")
                time.sleep(self.reconnect_delay)

    def stop(self):
        """
        Stops the WebSocket connection and terminates the thread.
        """
        self._running = False
        if self._ws:
            self._ws.close()
        if self._thread:
            self._thread.join()
        logger.info(f"Stopped WebSocket source for topic '{self.topic}'.")

    def default_topic(self):
        """
        Returns the topic configuration.
        """
        return {
            "name": self.topic,
            "key_serializer": self.key_serializer,
            "key_deserializer": self.key_deserializer,
            "value_serializer": self.value_serializer,
            "value_deserializer": self.value_deserializer
        }

    def _on_open(self, ws):
        logger.info("WebSocket connection opened.")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket connection closed with code: {close_status_code}, message: {close_msg}")
        # Let the _start_websocket loop handle reconnect if necessary.

    def _on_error(self, ws, error):
        logger.error(f"WebSocket encountered an error: {error}")

    def _default_on_message(self, ws, message):
        """
        Default message handler if no custom on_message handler is provided.
        """
        try:
            data = self.value_deserializer(message)
            logger.info(f"Received message on topic '{self.topic}': {data}")
            # Simulate data production (e.g., send to a producer)
            # If an error occurs during production, call the producer error callback
            try:
                if self.on_message_processed:
                    self.on_message_processed(data)
                logger.info("Message processed successfully.")
            except Exception as production_error:
                logger.error(f"Error during message production: {production_error}")
                if self.on_producer_error:
                    self.on_producer_error(production_error)

        except Exception as e:
            logger.error(f"Failed to deserialize message: {message} with error: {e}")
