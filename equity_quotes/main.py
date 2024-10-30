import json
import logging
import os
from typing import Any, Dict

import websockets.sync.client as ws
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.models.topics import Topic
from quixstreams.sources import Source

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class PolygonSource(Source):

    def __init__(self, name: str):
        super().__init__(name=name)
        self.running = True
        self._producer_topic = Topic(
            name=os.environ["OUTPUT"] or "equity-quotes",
            key_serializer="str",
            value_serializer="json"
        )

    def map_fields(self, data: Dict[Any, Any]) -> dict:
        record = {}
        record["sym"] = data.get("sym")
        record["event"] = data.get("ev")
        record["day_vol"] = data.get("av")
        record["day_open"] = data.get("op")
        record["vol"] = data.get("v")
        record["open"] = data.get("o")
        record["close"] = data.get("c")
        record["high"] = data.get("h")
        record["low"] = data.get("l")
        record["avg_qty"] = data.get("z")
        record["start_ts"] = data.get("s")
        record["end_ts"] = data.get("e")
        record["vwap"] = data.get("vw")
        return record

    def run(self):
        while self.running:
            with ws.connect(os.environ.get('WEBSOCKET_ENDPOINT')) as websocket:
                logger.info("Connecting to the Polygon Websockets API.")
                auth_message = json.dumps({"action": "auth", "params": os.environ.get('POLYGON_API_KEY')})
                websocket.send(auth_message)
                logger.info("Successfully authenticated with the Polygon API.")
                subscribe_message = json.dumps({"action": "subscribe", "params": "A.*"})
                websocket.send(subscribe_message)
                logger.info("Successfully subscribed to the aggregate p/second channel for all equities.")
                for message in websocket:
                    try:
                        data = json.loads(message)
                        if data.get("ev") is not None:
                            data = self.map_fields(data)
                            msg = self.serialize(key=data.get('sym'), value=data, timestamp_ms=data.get('t'), headers={"data_provider": "Polygon"})
                            self.produce(key=msg.key, value=msg.value, poll_timeout=2.0, buffer_error_max_tries=3, timestamp=msg.timestamp_ms, headers=msg.headers)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")


app = Application(consumer_group="equity-quotes", auto_create_topics=False)

topic_name = os.environ["OUTPUT"]
topic = app.topic(topic_name)
sdf = app.dataframe(source=PolygonSource(name=topic.name))
sdf.print()


def main():
    app = Application(consumer_group="equity-quotes", auto_create_topics=False)
    topic_name = os.environ["OUTPUT"]
    topic = app.topic(topic_name)
    sdf = app.dataframe(source=PolygonSource(name=topic.name))
    sdf.print()
    app.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
