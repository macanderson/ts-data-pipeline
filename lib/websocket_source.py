import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Union

import quixstreams as qx
import websocket
from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout
from quixstreams.models.messages import KafkaMessage
from quixstreams.models.topics import Topic
from quixstreams.models.types import Headers
from quixstreams.rowproducer import RowProducer

logger = logging.getLogger(__name__)

@dataclass(slots=True)
class SourceConfig:
    connection: qx.kafka.configuration.ConnectionConfig
    topic_name: str
    key_serializer: str
    value_serializer: str
    key_deserializer: str
    value_deserializer: str
    websocket_uri: str
    websocket_auth_message: str
    websocket_subscribe_message: str = json.dumps({"action": "subscribe"})
    websocket_unsubscribe_message: str = json.dumps({"action": "unsubscribe"})


class ExternalSource(ABC):

,.""
    def __init__(self, config: SourceConfig):
        self.config = config
        self.running = False
        self.ws_connect = websocket.WebSocketApp(
            self.config.websocket_uri,
            on_message=self.ws_on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.kafka_output = Topic(
            name=self.config.topic_name,
            key_serializer=self.config.key_serializer,
            value_serializer=self.config.value_serializer,
        )
        self.producer = RowProducer(self.config.connection, self.kafka_output)


    def on_error(self, ws: websocket.WebSocketApp, error: str):
        self.logger.error(f"Websocket error: {error}")
        self.producer.flush()
        self.ws_connect.close()

    def on_close(self, ws: websocket.WebSocketApp):
        self.running = False
        self.ws_connect.close()
        self.producer.flush()
        self.producer.close()

    def ws_on_message(self, ws: websocket.WebSocketApp, message: str):
        logger.debug(f"Websocket message: {message}")
        self.on_message(ws, message)

    @abstractmethod
    def on_message(self, ws: websocket.WebSocketApp, message: str):
        pass

    @abstractmethod
    def output_topic(self) -> Topic:
        pass

    @abstractmethod
    def start(self, message: KafkaMessage) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass



    @abstractmethod
    def start(self) -> None:
        """
        This is the base class for all sources.

    Sources are executed in a sub-process of the main application.

    To create your own source you need to implement:

    * `start`
    * `stop`
    * `default_topic`

    `BaseSource` is the most basic interface, and the framework expects every
    source to implement it.
    Use `Source` to benefit from a base implementation.

    You can connect a source to a StreamingDataframe using the Application.

    Example snippet:

    ```python
    class RandomNumbersSource(BaseSource):
    def __init__(self):
        super().__init__()
        self._running = False

    def start(self):
        self._running = True

        while self._running:
            number = random.randint(0, 100)
            serialized = self._producer_topic.serialize(value=number)
            self._producer.produce(
                topic=self._producer_topic.name,
                key=serialized.key,
                value=serialized.value,
            )

    def stop(self):
        self._running = False

    def default_topic(self) -> Topic:
        return Topic(
            name="topic-name",
            value_deserializer="json",
            value_serializer="json",
        )


    def main():
        app = Application(broker_address="localhost:9092")
        source = RandomNumbersSource()

        sdf = app.dataframe(source=source)
        sdf.print(metadata=True)

        app.run()


    if __name__ == "__main__":
        main()
    ```
    """

    # time in seconds the application will wait for the source to stop.
    shutdown_timeout: float = 10

    def __init__(self):
        self._producer: Optional[RowProducer] = None
        self._producer_topic: Optional[Topic] = None
        self._configured: bool = False

    def configure(self, topic: Topic, producer: RowProducer) -> None:
        """
        This method is triggered when the source is registered to the Application.

        It configures the source's Kafka producer and the topic it will produce to.
        """
        self._producer = producer
        self._producer_topic = topic
        self._configured = True

    @property
    def configured(self):
        return self._configured

    @property
    def producer_topic(self):
        return self._producer_topic

    @abstractmethod
    def start(self) -> None:
        """
        This method is triggered in the subprocess when the source is started.

        The subprocess will run as long as the start method executes.
        Use it to fetch data and produce it to Kafka.
        """

    @abstractmethod
    def stop(self) -> None:
        """
        This method is triggered when the application is shutting down.

        The source must ensure that the `run` method is completed soon.
        """

    @abstractmethod
    def default_topic(self) -> Topic:
        """
        This method is triggered when the topic is not provided to the source.

        The source must return a default topic configuration.
        """


class Source(BaseSource):
    """
    A base class for custom Sources that provides a basic implementation of `BaseSource`
    interface.
    It is recommended to interface to create custom sources.

    Subclass it and implement the `run` method to fetch data and produce it to Kafka.

    Example:

    ```python
    from quixstreams import Application
    import random

    from quixstreams.sources import Source


    class RandomNumbersSource(Source):
        def run(self):
            while self.running:
                number = random.randint(0, 100)
                serialized = self._producer_topic.serialize(value=number)
                self.produce(key=str(number), value=serialized.value)


    def main():
        app = Application(broker_address="localhost:9092")
        source = RandomNumbersSource(name="random-source")

        sdf = app.dataframe(source=source)
        sdf.print(metadata=True)

        app.run()


    if __name__ == "__main__":
        main()
    ```


    Helper methods and properties:

    * `serialize()`
    * `produce()`
    * `flush()`
    * `running`
    """

    def __init__(self, name: str, shutdown_timeout: float = 10) -> None:
        """
        :param name: The source unique name. Used to generate the topic configurtion
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown
        """
        super().__init__()

        # used to generate a unique topic for the source.
        self.name = name

        self.shutdown_timeout = shutdown_timeout
        self._running = False

    @property
    def running(self) -> bool:
        """
        Property indicating if the source is running.

        The `stop` method will set it to `False`. Use it to stop the source gracefully.
        """
        return self._running

    def cleanup(self, failed: bool) -> None:
        """
        This method is triggered once the `run` method completes.

        Use it to clean up the resources and shut down the source gracefully.

        It flushes the producer when `_run` completes successfully.
        """
        if not failed:
            self.flush(self.shutdown_timeout / 2)

    def stop(self) -> None:
        """
        This method is triggered when the application is shutting down.

        It sets the `running` property to `False`.
        """
        self._running = False
        super().stop()

    def start(self):
        """
        This method is triggered in the subprocess when the source is started.

        It marks the source as running, execute it's run method and ensure cleanup happens.
        """
        self._running = True
        try:
            self.run()
        except BaseException:
            self.cleanup(failed=True)
            raise
        else:
            self.cleanup(failed=False)

    @abstractmethod
    def run(self):
        """
        This method is triggered in the subprocess when the source is started.

        The subprocess will run as long as the run method executes.
        Use it to fetch data and produce it to Kafka.
        """

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Headers] = None,
        timestamp_ms: Optional[int] = None,
    ) -> KafkaMessage:
        """
        Serialize data to bytes using the producer topic serializers and return a `quixstreams.models.messages.KafkaMessage`.

        :return: `quixstreams.models.messages.KafkaMessage`
        """
        return self._producer_topic.serialize(
            key=key, value=value, headers=headers, timestamp_ms=timestamp_ms
        )

    def produce(
        self,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = 5.0,
        buffer_error_max_tries: int = 3,
    ) -> None:
        """
        Produce a message to the configured source topic in Kafka.
        """

        self._producer.produce(
            topic=self._producer_topic.name,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp,
            poll_timeout=poll_timeout,
            buffer_error_max_tries=buffer_error_max_tries,
        )

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        This method flush the producer.

        It ensures all messages are successfully delivered to Kafka.

        :param float timeout: time to attempt flushing (seconds).
            None use producer default or -1 is infinite. Default: None

        :raises CheckpointProducerTimeout: if any message fails to produce before the timeout
        """
        logger.debug("Flushing source")
        unproduced_msg_count = self._producer.flush(timeout)
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
            )

    def default_topic(self) -> Topic:
        """
        Return a default topic matching the source name.
        The default topic will not be used if the topic has already been provided to the source.

        :return: `quixstreams.models.topics.Topic`
        """
        return Topic(
            name=self.name,
            value_deserializer="json",
            value_serializer="json",
        )

    def __repr__(self):
        return self.name
