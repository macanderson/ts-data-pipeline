import json
import logging
import os

from dotenv import load_dotenv
from quixplus import WebsocketSource
from quixstreams import Application

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def transform(data: dict) -> dict:
    """Transform the data to the expected format."""
    record = {
        "symbol": data.get("sym") or "none",
        "event": data.get("ev") or "none",
        "open": data.get("o") or 0,
        "high": data.get("h") or 0,
        "low": data.get("l") or 0,
        "close": data.get("c") or 0,
        "vwap": data.get("vw") or 0,
        "volume": data.get("v") or 0,
        "num_trades": data.get("z") or 0,
        "cum_volume": data.get("av") or 0,
        "ts": data.get("t") or 0,
    }
    print(f"record in transform: {record}")
    return record


def validate(data: dict) -> bool:
    """Validate the data."""
    return data.get("sym") is not None


"""Main function to run the application."""
topic_name = os.environ.get("OUTPUT", "equity-quotes")
ws_url = "wss://delayed.polygon.io/stocks"
auth_payload = {"action": "auth", "params": os.environ["POLYGON_TOKEN"]}
subscribe_payload = {"action": "subscribe", "params": "A.*"}

source = WebsocketSource(
    name="equity-quotes",
    ws_url=ws_url,
    auth_payload=auth_payload,
    subscribe_payload=subscribe_payload,
    validator=validate,
    transform=transform,
    key_field="symbol",
    timestamp_field="ts",
    debug=True,
)

# Set up the application
app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=False,
    loglevel=logging.INFO,
)

output_topic = app.topic(
    name=topic_name,
    key_serializer=str,
    value_serializer="json",
)


def main():

    logger.info(f"Adding source '{source.name}' to application. Output topic: '{output_topic.name}'")

    app.add_source(source=source, topic=output_topic)

    logger.info(f"Creating dataframe for topic '{output_topic.name}'")

    sdf = app.dataframe(output_topic)

    logger.info("Printing the dataframe")

    sdf.print(
        pretty=True,
        metadata=True,
    )

    logger.info("Running the application")
    app.run()


if __name__ == "__main__":
    try:
        logger.info("Starting application.")
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
