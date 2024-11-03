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
ws_url = "wss://socket.polygon.io"
auth_payload = {"action": "auth", "params": os.environ.get("POLYGON_TOKEN")}
subscribe_payload = {"action": "subscribe", "params": "A.*"}


source = WebsocketSource(
    topic_name=topic_name,
    ws_url=ws_url,
    key_serializer=str,
    value_serializer=json.dumps,
    reconnect_delay=5,
    transform=transform,
    validator=validate,
    auth_payload=auth_payload,
    subscribe_payload=subscribe_payload,
)


# Kafka Configuration
def main():
    """Main function to run the application."""

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
    )
    sdf = app.dataframe(source=source)
    sdf.print()
    app.run()

if __name__ == "__main__":
    try:
        logger.info("Starting application.")
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
