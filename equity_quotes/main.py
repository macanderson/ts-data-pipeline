import json
import logging
import os

from dotenv import load_dotenv
from quixplus import WebsocketSource
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def transform(data: dict) -> dict:
    """Transform the data to the expected format."""
    print(f"Received Polygon Websocket Data: {data}")

    record = {}

    record["symbol"] = data.get("sym") or "none"
    record["event"] = data.get("ev") or "none"
    record["volume"] = data.get("v") or 0
    record["open"] = data.get("o") or 0
    record["high"] = data.get("h") or 0
    record["low"] = data.get("l") or 0
    record["close"] = data.get("c") or 0
    record["vwap"] = data.get("vw") or 0
    record["trade_count"] = data.get("z") or 0
    record["day_volume"] = data.get("av") or 0
    return record


def validate(data: dict) -> bool:
    """Validate the data to ensure it is in the expected format."""

    return data.get("sym") is not None


topic_name = "equity-quotes"
ws_url = "wss://delayed.polygon.io/stocks"
auth_payload = {"action": "auth", "params": os.environ.get("POLYGON_TOKEN")}
subscribe_payload = {"action": "subscribe", "params": "A.*"}

def value_serializer(data: dict) -> bytes:
    """Serialize the data to bytes."""
    print(f"data in value serializer: {data}")
    return json.dumps(data).encode("utf-8")


source = WebsocketSource(
    name=topic_name,
    ws_url=ws_url,
    key_serializer=str,
    value_serializer=value_serializer,
    reconnect_delay=5,
    transform=transform,
    validator=validate,
    auth_payload=auth_payload,
    subscribe_payload=subscribe_payload,
    debug=True,
)


# Kafka Configuration
def main():
    """Main function to run the application."""
    print(os.environ["BOOTSTRAP_SERVERS"])
    connection = ConnectionConfig(
        bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"],
        sasl_mechanism=os.environ["SASL_MECHANISM"],
        security_protocol=os.environ["SECURITY_PROTOCOL"],
        sasl_username=os.environ["SASL_USERNAME"],
        sasl_password=os.environ["SASL_PASSWORD"],
    )

    # Set up the application
    app = Application(
        broker_address=connection,
        processing_guarantee="exactly-once",
        auto_create_topics=False,
    )
    app.add_source(source)
    app.run()

if __name__ == "__main__":
    try:
        logger.info("Starting application.")
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
