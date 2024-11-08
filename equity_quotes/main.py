import json
import logging
import os

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

from .websocket_source import WebsocketSource

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def transform(data: dict) -> dict:
    """Transform the data to the expected format."""
    record = {}
    record["symbol"] = data.get("sym") or "none"
    record["event"] = data.get("ev") or "none"
    record["open"] = data.get("o") or 0
    record["high"] = data.get("h") or 0
    record["low"] = data.get("l") or 0
    record["close"] = data.get("c") or 0
    record["vwap"] = data.get("vw") or 0
    record["volume"] = data.get("v") or 0
    record["num_trades"] = data.get("z") or 0
    record["cum_volume"] = data.get("av") or 0
    record["ts"] = data.get("t") or 0
    print(f"record in transform: {record}")
    return record

def validate(data: dict) -> bool:
    """Validate the data."""
    return dict.get("sym") is not None

def main():
    """Main function to run the application."""

    topic_name = "equity-quotes"
    ws_url = "wss://delayed.polygon.io/stocks"
    auth_payload = {"action": "auth", "params": os.environ.get("POLYGON_TOKEN")}
    subscribe_payload = {"action": "subscribe", "params": "A.*"}

    source = WebsocketSource(
        ws_url=ws_url,
        auth_payload=auth_payload,
        subscribe_payload=subscribe_payload,
        validator=validate,
        transform=transform,
        key_fields=['symbol'],
        timestamp_field='ts',
        debug=True
    )

    # Set up the application
    app = Application(
        broker_address=None,
        processing_guarantee="exactly-once",
        auto_create_topics=False,
    )

    output_topic = app.topic(
        name=topic_name,
        key_serializer=str,
        value_serializer='json',
    )

    app.add_source(source=source, topic=output_topic)

    logger.info("Adding source to the application.")

    sdf = app.dataframe(output_topic)

    sdf.print(pretty=True)

    app.run()


if __name__ == "__main__":
    try:
        logger.info("Starting application.")
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
