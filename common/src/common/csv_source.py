"""
A CSV source that reads data from a single CSV file and produces messages
with a JSON payload and optionally a composite key.
"""
import csv
import json
import logging
from typing import Any, Callable, List, Optional

from quixstreams.models.topics import Topic
from quixstreams.sources.base import Source

logger = logging.getLogger(__name__)


class CSVSource(Source):
    def __init__(
        self,
        path: str,
        key_columns: Optional[List[str]] = None,
        key_separator: str = "_",
        dialect: str = "excel",
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        key_serializer: Callable[[Any], str] = str,
        value_serializer: Callable[[Any], str] = json.dumps,
    ) -> None:
        """
        A CSV source that reads data from a single CSV file and produces messages
        with a JSON payload and optionally a composite key.

        :param path: Path to the CSV file.
        :param key_columns: List of columns to construct the Kafka message key. Optional.
        :param key_separator: Separator to use between key column values if multiple are specified.
        :param dialect: CSV dialect to use for parsing the file. Default is "excel".
        :param key_serializer: Serializer for the message key.
        :param value_serializer: Serializer for the message value.
        :param shutdown_timeout: Time to wait for graceful shutdown.
        """
        super().__init__(name or path, shutdown_timeout)
        self.path = path
        self.key_columns = key_columns or []
        self.key_separator = key_separator
        self.dialect = dialect
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def run(self):
        with open(self.path, "r") as f:
            reader = csv.DictReader(f, dialect=self.dialect)

            while self.running:
                try:
                    item = next(reader)
                except StopIteration:
                    logger.info("Reached end of CSV file.")
                    return
                except Exception as e:
                    logger.error(f"Error reading CSV file: {e}")
                    return

                # Construct the key by concatenating specified columns
                key = (
                    self.key_separator.join([str(item[col]) for col in self.key_columns if col in item])
                    if self.key_columns
                    else None
                )

                try:
                    # Serialize the row as a JSON value
                    value = self.value_serializer(item)
                    msg = self.serialize(
                        key=self.key_serializer(key) if key else None,
                        value=value,
                        timestamp_ms=None  # Can add timestamp processing if needed
                    )
                    self.produce(
                        key=msg.key,
                        value=msg.value,
                        timestamp=msg.timestamp,
                        headers=msg.headers
                    )
                    logger.info(f"Produced message for key: {key}")
                except Exception as e:
                    logger.error(f"Error serializing or producing message: {e}")

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_serializer="json",
            value_deserializer="json",
        )