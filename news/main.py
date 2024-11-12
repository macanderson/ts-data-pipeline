"""
This module sets up a news polling application using the Polygon API to fetch ticker news
and stream it to a specified output topic. The application is designed to run continuously,
polling for new articles at regular intervals and processing them for downstream consumption.
"""

import logging
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from polygon import RESTClient
from polygon.exceptions import AuthError, BadResponse
from quixstreams import Application
from quixstreams.models import Topic
from quixstreams.sources.base.source import Source

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Application:
app = Application(
    broker_address=None,
    processing_guarantee="at-least-once",
    auto_create_topics=False,
    consumer_group="tsdp-news"
)

# Output topic:
output = Topic(
    name=os.environ["OUTPUT"],
    key_serializer="str",
    value_serializer="json"
)


class TickerNewsSource(Source):
    """
    A source for fetching and streaming ticker news from the Polygon API.

    Attributes:
        client (RESTClient): The REST client for interacting with the Polygon API.
        last_polled (float): Timestamp of the last poll for news articles.
    """

    def __init__(self, name: str, shutdown_timeout: float = 10) -> None:
        """
        Initialize the TickerNewsSource with a name and optional shutdown timeout.

        Args:
            name (str): The name of the source.
            shutdown_timeout (float): The time to wait before shutting down the source.
        """
        super().__init__(name, shutdown_timeout)
        self.client = RESTClient(os.environ["POLYGON_API_KEY"])
        self.last_polled = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    def poll_news(self) -> None:
        """
        Poll the Polygon API for the latest ticker news and produce messages to the output topic.
        """
        try:
            self.last_polled = datetime.now().timestamp()
            article_counter = 0
            for n in self.client.list_ticker_news(published_utc_gt=self.last_polled, order="desc"):
                article_counter += 1
                key = n.id
                value = n
                timestamp = datetime.fromisoformat(n.published_utc).timestamp()
                headers = {
                    "publisher": n.publisher.name,
                    "published_date": timestamp.date().strftime("%Y-%m-%d"),
                    "tickers": [t for t in n.tickers]
                }
                msg = self.serialize(
                    key=key,
                    value=value,
                    headers=headers,
                    timestamp_ms=timestamp
                )
                self.produce(
                    key=msg.key,
                    value=msg.value,
                    headers=msg.headers,
                    timestamp=msg.timestamp_ms
                )
                logger.debug("Produced news article: %s", key)
        except BadResponse as e:
            logger.error("ApiException:Error fetching news: %s", e)
        except AuthError as e:
            logger.error("AuthError:Error fetching news: %s", e)
        except Exception as e:
            logger.error("Error fetching news: %s", e)

    def run(self) -> None:
        """
        Continuously poll for news while the source is running.
        """
        while self.running:
            self.poll_news()
            time.sleep(10)


def main() -> None:
    """
    Main function to set up and run the news polling application.
    """
    app.add_source(source=TickerNewsSource(name=output.name), topic=output)
    app.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")

__all__ = ["TickerNewsSource"]