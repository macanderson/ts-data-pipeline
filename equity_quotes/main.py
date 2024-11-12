import logging
import os
import sys
import time
from functools import partial
from multiprocessing import Process
from pprint import pprint

from dotenv import load_dotenv
from quixplus.sources import WebsocketSource
from quixstreams import Application
from quixstreams.models import Topic

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

API_TOKEN = os.environ["POLYGON_TOKEN"]

if not API_TOKEN:
    raise ValueError("POLYGON_TOKEN environment variable is not set.")

# Set up the application
app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=False,
    loglevel=logging.INFO,
)

output_topic = Topic(
    name=os.environ.get("OUTPUT", "equity-quotes"),
    key_serializer=str,
    value_serializer="json",
)

WS_URL = "wss://delayed.polygon.io/stocks"
AUTH_PAYLOAD = {"action": "auth", "params": API_TOKEN}
SUBSCRIBE_PAYLOAD = {"action": "subscribe", "params": "A.*"}


def key_func(data: dict) -> dict:
    return {"id": data.get("id")}


def timestamp_func(data: dict) -> int:
    return int(data.get("timestamp", time.time() * 1000))


def custom_headers_func(data: dict) -> dict:
    return {
        "X-Data-Provider": "polygon",
        "X-System-Platform": sys.platform,
        "X-System-Python": sys.version,
        "X-System-Python-Version": sys.version_info,
    }


def transform(ctx: WebsocketSource, data: dict) -> dict:
    """Transform the data to the expected format."""
    pprint(data)
    data = {
        "symbol": data.get("sym") or "none",
        "event": data.get("ev") or "none",
        "open": data.get("o") or 0,
        "high": data.get("h") or 0,
        "low": data.get("l") or 0,
        "close": data.get("c") or 0,
        "vwap": data.get("vw") or 0,
        "bar_volume": data.get("v") or 0,
        "num_of_trades": data.get("z") or 0,
        "session_volume": data.get("av") or 0,
        "timestamp": data.get("t") or 0,
    }
    return data


def validate_message(msg):
    if isinstance(msg, dict):
        return "sym" in msg
    elif isinstance(msg, list):
        return all(isinstance(item, dict) and "sym" in item for item in msg)
    return False


source = WebsocketSource(
    name=output_topic.name,
    ws_url=WS_URL,
    auth_payload=AUTH_PAYLOAD,
    subscribe_payload=SUBSCRIBE_PAYLOAD,
    key_func=key_func,
    timestamp_func=timestamp_func,
    headers_func=custom_headers_func,
    transform=transform,
    validator=validate_message,
    debug=True,
)


def run_app():
    logger.info(
        f"Adding source '{source.name}' to application. Output topic: '{output_topic.name}'"
    )
    app.add_source(source=source, topic=output_topic)
    logger.info("Running the application")
    app.run()


def main():
    # Use partial to avoid pickling issues with lambda functions
    run_app_partial = partial(run_app)
    process = Process(target=run_app_partial)
    process.start()
    process.join()

if __name__ == "__main__":
    try:
        logger.info("Starting application.")
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
