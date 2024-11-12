"""
Equity quotes application.
"""

import json
import logging
import os
import sys
import time
from multiprocessing import Process
from pprint import pprint

from dotenv import load_dotenv
from quixplus.sources.tornado_websocket import TornadoWebsocketSource as WebsocketSource
from quixstreams import Application
from quixstreams.models import Topic

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

API_TOKEN = os.getenv("POLYGON_TOKEN")

if not API_TOKEN:
    raise ValueError("POLYGON_TOKEN environment variable is not set.")

WS_URL = "wss://delayed.polygon.io/stocks"
AUTH_PAYLOAD = {"action": "auth", "params": API_TOKEN}
SUBSCRIBE_PAYLOAD = {"action": "subscribe", "params": "A.*"}


def key_func(data: dict) -> dict:
    return {"id": data.get("id")}


def timestamp_func(data: dict) -> int:
    return int(data.get("timestamp", time.time() * 1000))


def headers_func(data: dict) -> dict:
    return {
        "X-Data-Provider": "polygon",
        "X-System-Platform": sys.platform,
        "X-System-Python": sys.version,
        "X-System-Python-Version": sys.version_info,
    }


def transform_func(data: dict) -> dict:
    """Transform the data to the expected format."""
    pprint(data)
    return {
        "symbol": data.get("sym", "none"),
        "event": data.get("ev", "none"),
        "open": data.get("o", 0),
        "high": data.get("h", 0),
        "low": data.get("l", 0),
        "close": data.get("c", 0),
        "vwap": data.get("vw", 0),
        "bar_volume": data.get("v", 0),
        "num_of_trades": data.get("z", 0),
        "session_volume": data.get("av", 0),
        "timestamp": data.get("t", 0),
    }


def validate_message(data: dict) -> bool:
    print(data)
    if isinstance(data, dict):
        return "sym" in data
    elif isinstance(data, list):
        return all(isinstance(item, dict) and "sym" in item for item in data)
    return False




def main():
    """
    Run the application.
    """

    # Set up the application
    app = Application(
        broker_address=None,
        processing_guarantee="exactly-once",
        auto_create_topics=False,
        loglevel=logging.DEBUG,
    )

    output_topic = Topic(
        name=os.getenv("OUTPUT", "equity-quotes"),
        key_serializer=str,
        value_serializer="json",
    )

    source = WebsocketSource(
        name=output_topic.name,
        url=WS_URL,
        auth_payload=AUTH_PAYLOAD,
        subscription_payloads=[SUBSCRIBE_PAYLOAD],
        key_func=key_func,
        timestamp_func=timestamp_func,
        headers_func=headers_func,
        value_func=transform_func,
        validator_func=validate_message,
        debug=True,
    )

    logger.info("Adding source to application")

    app.add_source(source=source, topic=output_topic)
    
    logger.info("Running the application container...")
    app.run()


if __name__ == "__main__":
    try:
        logger.info("Starting application.")
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
