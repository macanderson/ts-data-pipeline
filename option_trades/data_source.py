import json
import logging
from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from quixstreams.models import TimestampType
from quixstreams.models.topics import Topic
from quixstreams.sources.base.source import BaseSource

# from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout
logger = logging.getLogger(__name__)

MessageKey = Union[str, bytes, dict] | None
MessageValue = Union[str, bytes, dict]
HeaderValue = Optional[Union[str, bytes]]
MessageHeadersTuples = List[Tuple[str, HeaderValue]]
MessageHeadersMapping = Dict[str, HeaderValue]
Headers = Dict | None

class KafkaMessage:

    def __init__(
        self,
        key: Optional[MessageKey],
        value: Optional[MessageValue],
        headers: dict,
        timestamp_ms: int | None = None
        ):
        self.key = self.__process_value(key)
        self.value = self._process_value(value)
        self.headers = headers
        self.timestamp_ms = timestamp_ms if timestamp_ms is not None else int(datetime.now().timestamp() * 1000)

    def _process_value(
        self,
        value: Any) -> Any:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, dict):
            return json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value.encode("utf-8")
        return value


def extract_timestamp(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,  # noqa: E302
) -> int:  #  noqa: E302
    """Extract the timestamp from the message."""
    return value.get("ts") or 0


class CustomSource(BaseSource):
    """
    A custom source that fetches data from a websocket and produces it to Kafka.
    """

    def __init__(self, name: str, shutdown_timeout: float=10) -> None:
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
        key: Optional[object]=None,
        value: Optional[object]=None,
        headers: Optional[Headers]=None,
        timestamp_ms: Optional[int]=None,
    ) -> KafkaMessage:
        """
        Serialize data to bytes using the producer topic serializers and return a `quixstreams.models.messages.KafkaMessage`.

        :return: `quixstreams.models.messages.KafkaMessage`
        """
        # return KafkaMessage(
        #     key=key,
        #     value=value,
        #     headers=headers,
        #     timestamp_ms=timestamp_ms
        # )
        return self._producer_topic.serialize(
            key=key, value=value, headers=headers, timestamp_ms=timestamp_ms
        )

    def produce(
        self,
        value: Optional[Union[str, bytes]]=None,
        key: Optional[Union[str, bytes]]=None,
        headers: Optional[Headers]=None,
        partition: Optional[int]=None,
        timestamp: Optional[int]=None,
        poll_timeout: float=5.0,
        buffer_error_max_tries: int=3,
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

    def flush(self, timeout: Optional[float]=None) -> None:
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
        print(f"Inside of the default_topic method -> Source name: {self.name}")
        return Topic(
            name=self.name,
            value_deserializer="json",
            value_serializer="json",
            key_serializer="str",
            key_deserializer="str",
            timestamp_extractor=extract_timestamp,
        )

    def __repr__(self):
        return self.name
