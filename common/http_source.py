"""
Custom HTTP Source for Quixstreams
----------------------------------
Prerequisies:

    1. Install the required packages:
        `pip install quixstreams requests apscheduler`
    2. Create a new Python file and paste the code below.
    3. Replace the placeholders with your own values.
    4. Run the script to start the HTTP polling source.

Explanation of Features
-----------------------
This class extends the Quixstreams Source class to create a custom HTTP polling source.
It fetches data from a URL at regular intervals, extracts key-value pairs using JSONPath,
and produces messages to a Kafka topic.

	1.	JSON Path Selection:
        •	`key_json_path` and `value_json_path` accept JSONPath expressions
            for extracting parts of the response.
        •	If `value_json_path` is not provided, the entire JSON response is
            used as the Kafka message value.
	2.	Cron-style Scheduling:
        •	You can specify `schedule_cron` as a cron expression
            (e.g., "0 9 * * *" to poll only at 9 AM daily).
        •	Uses apscheduler to run the `_start_polling` method at scheduled times,
            keeping the source active outside those hours.
	3.	Fault Tolerance:
        •	Robust error handling logs issues with polling, JSON path extraction,
            serialization, and data production without interrupting the polling cycle.
	4.	Custom Serializers:
        •	`key_serializer` and `value_serializer` support custom functions
            for flexible data formatting.
"""
import asyncio
import json
import logging
from typing import Optional, Callable, Dict, Any, Union

from quixstreams.models.topics import Topic
from quixstreams.sources.base import Source

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from jsonpath_ng import parse
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)
# Configure logger to pretty print
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

class AIOHTTPSource(Source):
    """Custom Http Source for Quixstreams cron-like scheduling."""
    def __init__(
        self,
        url: str,
        poll_interval: float=5.0,
        auth_type: Optional[str]=None,
        auth_credentials: Optional[Union[str, Dict[str, str]]]=None,
        key_json_path: Optional[str]=None,
        value_json_path: Optional[str]=None,
        schedule_cron: Optional[str]=None,
        name: Optional[str]=None,
        shutdown_timeout: float=10,
        key_serializer: Callable[[Any], str]=str,
        value_serializer: Callable[[Any], str]=json.dumps,
    ) -> None:
        """
        Initializes an HTTP polling source.

        :param url: The URL to poll.
        :param poll_interval: How frequently to poll the endpoint, in seconds.
        :param auth_type: Type of authentication ('bearer', 'basic', or custom headers).
        :param auth_credentials: Authentication credentials.
        :param key_json_path: JSONPath to extract the key for the Kafka message.
        :param value_json_path: JSONPath to extract the value for the Kafka message.
        :param schedule_cron: Cron-style schedule string to limit polling to specific hours.
        :param name: The name of the source.
        :param shutdown_timeout: Time to wait for a graceful shutdown.
        :param key_serializer: Serializer for the message key.
        :param value_serializer: Serializer for the message value.
        """
        super().__init__(name or url, shutdown_timeout)
        self.url = url
        self.poll_interval = poll_interval
        self.auth_type = auth_type
        self.auth_credentials = auth_credentials
        self.key_json_path = parse(key_json_path) if key_json_path else None
        self.value_json_path = parse(value_json_path) if value_json_path else None
        self.schedule_cron = schedule_cron
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.scheduler = AsyncIOScheduler()
        if schedule_cron:
            self.scheduler.add_job(self._start_polling, CronTrigger.from_crontab(schedule_cron))

    async def _get_auth_headers(self) -> Dict[str, str]:
        """Returns the appropriate headers based on the auth_type and credentials."""
        headers = {}
        if self.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.auth_credentials}"
        elif self.auth_type == "basic":
            from aiohttp.helpers import BasicAuth
            headers["Authorization"] = BasicAuth(*self.auth_credentials).encode()
        elif self.auth_type == "custom" and isinstance(self.auth_credentials, dict):
            headers.update(self.auth_credentials)
        return headers

    async def poll_endpoint(self):
        """Polls the endpoint at specified intervals, processes, and produces data."""
        async with aiohttp.ClientSession() as session:
            while self.running:
                try:
                    headers = await self._get_auth_headers()
                    async with session.get(self.url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()

                            # Extract and serialize the key
                            key = self._extract_json_path(data, self.key_json_path)
                            serialized_key = self.key_serializer(key) if key else None

                            # Extract and serialize the value
                            value = self._extract_json_path(data, self.value_json_path) or data
                            serialized_value = self.value_serializer(value)

                            msg = self.serialize(key=serialized_key, value=serialized_value)
                            self.produce(
                                key=msg.key,
                                value=msg.value,
                                timestamp=msg.timestamp,
                                headers=msg.headers
                            )
                            logger.info(f"Produced message for key: {serialized_key}")
                        else:
                            logger.error(f"Failed to fetch data: HTTP {response.status}")
                except Exception as e:
                    logger.error(f"Error during polling or message production: {e}")

                await asyncio.sleep(self.poll_interval)

    def _extract_json_path(self, data: dict, json_path) -> Optional[Any]:
        """Extracts data from JSON using JSONPath."""
        if not json_path:
            return None
        try:
            matches = json_path.find(data)
            return matches[0].value if matches else None
        except Exception as e:
            logger.error(f"Error extracting JSON path {json_path} from data: {e}")
            return None

    async def _start_polling(self):
        """Starts the polling task asynchronously within scheduled hours if defined."""
        await self.poll_endpoint()

    def run(self):
        """Runs the polling loop using asyncio and honors the scheduling if provided."""
        if self.schedule_cron:
            self.scheduler.start()
            asyncio.get_event_loop().run_forever()
        else:
            asyncio.run(self.poll_endpoint())

    def stop(self):
        """Stops polling and shuts down the scheduler if it's running."""
        self.running = False
        if self.scheduler.running:
            self.scheduler.shutdown()
        logger.info("HTTP source stopped.")

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_serializer="json",
            value_deserializer="json",
        )
