from datetime import datetime
import json
import os

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

from data_queue import DataQueue


load_dotenv()

class DataConsumer():
    """DataConsumer consumes data from a Kafka topic and publishes it to a DataQueue."""
    def __init__(self, queue: DataQueue) -> None:
        self.queue = queue
        self.data = []
        self.connection = ConnectionConfig(
            bootstrap_servers=os.environ["KAFKA_BROKER_ADDRESS"],
            sasl_mechanism=os.environ["KAFKA_SASL_MECHANISM"],
            security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
            sasl_username=os.environ["KAFKA_KEY"],
            sasl_password=os.environ["KAFKA_SECRET"],
        )

        self.app = Application(
            broker_address=self.connection,
            auto_offset_reset="earliest",
            consumer_group="market_dashboard",
            group_id="market_dashboard",
            use_changelog_topics=True,
        )
        self.consumer = self.app.get_consumer()
        self.topic_name = os.environ["INPUT"]
        self.topic = self.app.topic(self.topic_name)
        self.run = False
        self.cols = []

    def get_available_params(self):
        if self.data:
            return list(self.data[-1].keys())
        return []

    # subscription is moved to start() to give the client of this code more control
    # over when to start receiving data. You can move this logic to constructor if
    # necessary.
    def start(self):
        self.run = True
        with self.consumer:
            self.consumer.subscribe([self.topic.name])
            while self.run:
                msg = self.consumer.poll(timeout=1.0)
                if msg is not None:
                    # Decode the message data from bytes to string
                    message_data = msg.value().decode('utf-8')
                    # Convert the string to a dictionary
                    data_dict = json.loads(message_data)

                    # Now you can check for the timestamp column in data_dict
                    # but first lets work out what the timestamp column is called
                    timestamp_column_options = ["timestamp", "Timestamp", "time", "ts"]
                    t_stamp_col = ""
                    for ts_col in timestamp_column_options:
                        if ts_col in data_dict:
                            t_stamp_col = ts_col
                            break

                    # Assuming you want to convert the timestamp to a datetime object
                    if t_stamp_col:
                        timestamp = int(data_dict[t_stamp_col]) / 1e9  # Assuming the timestamp is in nanoseconds
                        data_dict["datetime"] = datetime.fromtimestamp(timestamp)

                    # Append data to the existing list
                    self.data.append(data_dict)

                    # Publish a reference to the data list
                    self.queue.put(self.data)


    def stop(self):
        self.consumer.unsubscribe()
        self.run = False
