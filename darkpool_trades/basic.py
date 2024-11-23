"""
Basic Kafka producer and consumer.
"""

import logging
import os
from datetime import datetime
from typing import Dict, Optional

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.schema_registry import (
    RegisteredSchema,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import (
    AvroSerializer,
)
from confluent_kafka.serialization import (
    SerializationContext,
    StringSerializer,
)
from dotenv import load_dotenv

load_dotenv()


string_serializer = StringSerializer()
avro_serializer = AvroSerializer(SchemaRegistryClient({
    'url': os.environ['SCHEMA_REGISTRY_URL'],
    'basic.auth.user.info': os.environ['SCHEMA_REGISTRY_AUTH_INFO'],
}), "", None)


class KafkaManager:
    """
    Kafka manager class.
    """

    def __init__(
        self,
        extra_config: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the KafkaManager with additional configuration.
        """
        # initialize logger
        if logger is None:
            logger = logging.getLogger(__name__)
        self._logger = logger
        self._logger.info("Initializing KafkaManager")

        # default kafka config
        self._kafka_default_config = {
            'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            'security.protocol': os.environ['KAFKA_SECURITY_PROTOCOL'],
            'sasl.mechanism': os.environ['KAFKA_SASL_MECHANISM'],
            'sasl.username': os.environ['KAFKA_SASL_USERNAME'],
            'sasl.password': os.environ['KAFKA_SASL_PASSWORD'],
        }

        if extra_config:
            # update the default config with any extra config
            self._kafka_default_config.update(extra_config)

        self._schema_registry_client = SchemaRegistryClient({
            # schema registry url
            'url': os.environ['SCHEMA_REGISTRY_URL'],
            # authentication credentials are in the format username:password
            'basic.auth.user.info': os.environ['SCHEMA_REGISTRY_AUTH_INFO'],
        })

    @property
    def client_config(self):
        """
        Get the default configuration.
        """
        return self._kafka_default_config

    @property
    def schema_registry_client(self):
        """
        Get the schema registry client.
        """
        return self._schema_registry_client

    def get_latest_schema_for_topic(self, topic: str) -> RegisteredSchema:
        """
        Get the latest schema for a topic.

        Args:
            topic (str): The topic to get the latest schema for.

        Returns:
            RegisteredSchema: The latest schema for the topic.

        Raises:
            ValueError: If no schema is found for the topic.
        """
        subjects = self._schema_registry_client.get_subjects()
        for subject in subjects:
            if subject.startswith(topic):
                versions = self._schema_registry_client.get_versions(subject)
                latest_version = \
                    self._schema_registry_client.get_latest_version(subject)
                latest_version = versions[-1]
                schema_id = latest_version.schema_id
                schema = self._schema_registry_client.get_schema(schema_id)
                return schema.schema
        raise ValueError(f"No schema found for topic: {topic}")



class AvroProducer(ConfluentProducer):
    """
    A Kafka producer with Avro serialization capabilities.

    Args:
        topic (str): The topic to produce messages to.
    """

    def __init__(self, topic: str):
        self._kafka_manager = KafkaManager()
        self._kafka_config = self._kafka_manager.client_config
        self._key_serializer = self._kafka_config.pop('key.serializer', None)
        self._value_serializer = self._kafka_config.pop('value.serializer', None)

        super(AvroProducer, self).__init__(self._kafka_config)
        self._schema_registry_client = SchemaRegistryClient({
            'url': os.environ['SCHEMA_REGISTRY_URL'],
            'basic.auth.user.info': os.environ['SCHEMA_REGISTRY_AUTH_INFO'],
        })
        self._topic = topic
        self._schema = self._get_latest_schema_for_topic(topic)

    def _get_latest_schema_for_topic(self, topic: str) -> RegisteredSchema:
        """
        Get the latest schema for a topic.

        Args:
            topic (str): The topic to get the latest schema for.

        Returns:
            RegisteredSchema: The latest schema for the topic.

        Raises:
            ValueError: If no schema is found for the topic.
        """
        subjects = self._schema_registry_client.get_subjects()
        for subject in subjects:
            if subject.startswith(topic):
                latest_version = self._schema_registry_client.\
                        get_latest_version(subject)
                schema_id = latest_version.schema_id
                schema = self._schema_registry_client.get_schema(schema_id)
                return schema
        raise ValueError(f"No schema found for topic: {topic}")

    def poll(self, timeout_ms: int = 0) -> None:
        """
        Poll for events from the producer.
        """
        super(AvroProducer, self).poll(timeout_ms)

    def flush(self) -> None:
        """
        Flush the producer.
        """
        super(AvroProducer, self).flush()

    def produce(
        self,
        key: str,
        value: object,
        timestamp: float = 0,
        extra_headers: Optional[Dict[str, str]] = None
    ):
        """
        Produce a message to the Kafka topic.

        Args:
            key (str): Message payload key.

            value (Dict): Message payload value.

            timestamp (float, optional): Message timestamp (CreateTime) in
                milliseconds since Unix epoch UTC.

            extra_headers (Dict[str, str], optional): Additional headers to
                include in the message.

        Raises:

            BufferError: if the internal producer message queue is full.
                (``queue.buffering.max.messages`` exceeded). If this happens
                the application should call :py:func:`SerializingProducer.Poll`
                and try again.

            KeySerializationError: If an error occurs during key
                serialization.

            ValueSerializationError: If an error occurs during value
                serialization.

            KafkaException: For all other errors.
        """
        partition = -1
        topic = self._topic
        on_delivery = None

        if extra_headers is None:
            extra_headers = {}

        extra_headers.update({
            'created_at': datetime.now().isoformat(),
            'created_by': self.__class__.__name__,
        })

        ctx = SerializationContext(topic, MessageField.KEY, extra_headers)

        if self._key_serializer is not None:
            try:
                key = self._key_serializer(key, ctx)
            except Exception as se:
                raise KeySerializationError(se)

        ctx.field = MessageField.VALUE

        # Always use AvroSerializer for value serialization
        try:
            latest_schema = self._schema_registry_client.\
                    get_latest_version(self._topic + '-value')
            avro_serializer = AvroSerializer(
                self._schema_registry_client,
                latest_schema.schema.schema_str
            )
            print(latest_schema.schema.schema_str)

            value = avro_serializer(value, ctx)
        except Exception as se:
            raise ValueSerializationError(se)

        # produce the message
        super(AvroProducer, self).produce(
            topic,
            value,
            key,
            headers=extra_headers,
            partition=partition,
            timestamp=timestamp,
            on_delivery=on_delivery
        )


def main():
    """
    Main function to produce a message to the 'option-trades' topic.
    """
    topic = 'option-trades'
    producer = AvroProducer(topic)

    key = "sample_key"
    value = {
        "field1": "value1",
        "field2": "value2"
    }

    try:
        producer.produce(key=key, value=value)
        producer.flush()
        print(f"Message produced to topic '{topic}'")
    except Exception as e:
        print(f"Failed to produce message: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Starting basic Kafka producer and consumer")
    main()
