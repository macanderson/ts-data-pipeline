
import json
import logging
import threading
from datetime import datetime
from typing import Callable, Optional, Union

import websocket
from quixstream.sources.base.source import Source

logger = logging.getLogger(__name__)


class WebsocketSource(Source):
    """
    A source that reads messages from a WebSocket connection.
    Attributes:
        ws_url (str): The complete WebSocket URL to connect to.
        auth_payload (dict | None): The authentication message to send to the WebSocket.
        subscribe_payload (dict | None): The subscription message to send to the WebSocket.
        validator (Callable[[dict], bool] | None): A function to validate incoming messages.
        transform (Callable[[dict], dict] | None): A function to transform incoming messages.
    """

    def __init__(
        self,
        ws_url: str,
        auth_payload: Union[dict, None]=None,
        subscribe_payload: Union[dict, None]=None,
        validator: Optional[Callable[[dict], bool]]=None,
        transform: Optional[Callable[[dict], dict]]=None,
        key_fields: Optional[list[str]]=None,
        timestamp_field: Optional[str]=None,
        debug: bool=False
    ):
        super().__init__()
        logger.info(f"Initializing WebsocketSource with ws_url: {ws_url}")
        self.ws_url = ws_url
        self.auth_payload = auth_payload
        self.subscribe_payload = subscribe_payload
        self.validator: Optional[Callable[[dict], bool]] = validator
        self.transform: Optional[Callable[[dict], dict]] = transform
        self.key_fields = key_fields
        self.timestamp_field = timestamp_field
        self.debug = debug
        self.ws = None

    def on_open(self, ws):
        """
        Callback for when the WebSocket connection is opened.
        """
        logger.info("WebSocket connection opened")
        if hasattr(self, 'auth_payload') and self.auth_payload:
            ws.send(json.dumps(self.auth_payload))
            logger.info("Sent authentication payload")

        if hasattr(self, 'subscribe_payload') and self.subscribe_payload:
            ws.send(json.dumps(self.subscribe_payload))
            logger.info("Sent subscription payload")

    def on_message(self, ws, message):
        """
        Callback for when a message is received from the WebSocket.
        """
        if self.debug:
            logger.info(f"Received message: {message}")
        message_data = json.loads(message)
        if self.validator and not self.validator(message_data):
            logger.error(f"Invalid message: {message_data}")
            return

        if self.transform:
            message_data = self.transform(message_data)

        if self.key_fields:
            key_data = {field: message_data[field] for field in self.key_fields}
        else:
            key_data = {}

        if self.timestamp_field:
            timestamp = message_data[self.timestamp_field]
        else:
            timestamp = datetime.now()

        msg = self.serialize(
            key=key_data,
            message=message_data,
            timestamp=timestamp
        )
        self.produce(
            key=msg.key,
            message=msg.message,
            timestamp=msg.timestamp
        )

    def on_error(self, ws, error):
        """
        Callback for when an error occurs with the WebSocket.
        """
        logger.error(f"WebSocket error: {error}")
        self.running = False
        self.flush()
        self.reconnect()

    def reconnect(self):
        """
        Attempts to reconnect the WebSocket connection.
        """
        logger.info("Attempting to reconnect WebSocket")
        self.running = True
        self.run()

    def on_close(self, ws, close_status_code, close_msg):
        """
        Callback for when the WebSocket connection is closed.
        """
        logger.info("WebSocket connection closed")
        self.flush()
        self.running = False
        self.reconnect()

    def run(self):
        """
        Starts the WebSocket connection and sends random numbers.
        """
        while self.running:
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )

            wst = threading.Thread(target=self.ws.run_forever)
            wst.daemon = True
            wst.start()
        # finally close the connection
        self.ws.close()
