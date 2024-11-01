import json
import logging
import os
from typing import Any, Dict

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models.topics import Topic

from websocket_source import BaseWebSocketSource


load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

class PolygonSource(BaseWebSocketSource):
    def __init__(self, name: str):
        super().__init__(name=name)
        self.running = True
        self._producer_topic = Topic(
            name=os.environ.get("OUTPUT", "equity-quotes"),
            key_serializer="str",
            value_serializer="json"
        )

    def map_fields(self, data: Dict[Any, Any]) -> dict:
        """Transforms Polygon data into a structured format."""
        return {
            "sym": data.get("sym"),
            "event": data.get("ev"),
            "day_vol": data.get("av"),
            "day_open": data.get("op"),
            "vol": data.get("v"),
            "open": data.get("o"),
            "close": data.get("c"),
            "high": data.get("h"),
            "low": data.get("l"),
            "avg_qty": data.get("z"),
            "start_ts": data.get("s"),
            "end_ts": data.get("e"),
            "vwap": data.get("vw"),
        }

    def run(self):
        """Connects to the WebSocket, subscribes, and streams data to Kafka."""
        while self.running:
            try:
                with ws.connect(os.environ.get('WEBSOCKET_ENDPOINT')) as websocket:
                    logger.info("Connecting to the Polygon Websockets API.")
                    auth_message = json.dumps({
                        "action": "auth",
                        "params": os.environ.get('POLYGON_API_KEY')
                    })
                    websocket.send(auth_message)
                    logger.info("Successfully authenticated with the Polygon API.")

                    subscribe_message = json.dumps({
                        "action": "subscribe",
                        "params": "A.*"
                    })
                    websocket.send(subscribe_message)
                    logger.info("Subscribed to aggregate p/second channel for all equities.")

                    for message in websocket:
                        try:
                            data = json.loads(message)
                            if data.get("ev") is not None:
                                data = self.map_fields(data)
                                msg = self.serialize(
                                    key=data.get('sym'),
                                    value=data,
                                    timestamp_ms=data.get('start_ts'),
                                    headers={"data_provider": "Polygon"}
                                )
                                self.produce(
                                    key=msg.key,
                                    value=msg.value,
                                    poll_timeout=1.0,
                                    timestamp=msg.timestamp_ms,
                                    headers=msg.headers
                                )
                        except json.JSONDecodeError as e:
                            logger.error(f"Error decoding JSON message: {e}")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
            except Exception as e:
                logger.error(f"Connection error with WebSocket: {e}")

# Kafka Configuration
def main():
    connection = ConnectionConfig(
        bootstrap_servers=os.environ["KAFKA_BROKER_ADDRESS"],
        sasl_mechanism=os.environ["KAFKA_SASL_MECHANISM"],
        security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
        sasl_username=os.environ["KAFKA_KEY"],
        sasl_password=os.environ["KAFKA_SECRET"],
    )

    # Set up the application
    app = Application(
        broker_address=connection,
    )

    topic_name = os.environ["OUTPUT"]
    topic = app.topic(topic_name)
    sdf = app.dataframe(source=PolygonSource(name=topic.name))
    sdf.print()
    app.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Exiting application.")
