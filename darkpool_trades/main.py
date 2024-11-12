import logging
import os
from datetime import datetime
from typing import Dict, List

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.models import Topic
from quixstreams.sources.base.source import Source
from unusualwhales import Unusualwhales

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DarkpoolTradesSource(Source):
    """
    Source for fetching darkpool trades from UnusualWhales API.
    """

    def __init__(self, name: str):
        """
        Initialize the DarkpoolTradesSource with a name and UnusualWhales
        client.

        Args:
            name (str): The name of the source.
        """
        super().__init__(name)
        logger.info("Initializing UnusualWhales client")
        self.client = Unusualwhales(api_key=os.environ["UNUSUALWHALES_TOKEN"])
        self.last_polled = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    def poll_darkpool_trades(self) -> None:
        """
        Poll the UnusualWhales API for the latest darkpool trades and produce
        messages to the output topic.
        """
        logger.info("Polling UnusualWhales API for darkpool trades")
        self.last_polled = datetime.timestamp(datetime.now().date())
        with self.client.darkpool.with_raw_response.transactions.retrieve(
            published_utc_gt=self.last_polled,
            order="desc",
        ) as response:
            print(response.headers.get("X-My-Header"))

            for line in response.iter_lines():
                print(line)

    def run(self) -> List[Dict]:
        """
        Fetch data from the darkpool and print each trade.

        Returns:
            List[Dict]: A list of trade dictionaries.
        """
        # Fetch data from the darkpool
        logger.info("darkpool trades, running...")
        while self.running:
            for trade in self.client.get_darkpool_trades():
                print(trade)


def main():
    """
    Start the darkpool trades application.
    """

    logger.info("Starting darkpool trades application")

    app = Application(
        auto_create_topics=False,
        processing_guarantee="exactly-once",
    )

    output = Topic(
        name="darkpool_trades",
        key_serializer=str,
        value_serializer=json.dumps,
    )

    source = DarkpoolTradesSource(name="darkpool_trades")
    logger.info("Adding source to application")
    app.add_source(source=source, topic=output)
    logger.info("Running application...")
    app.run()


if __name__ == "__main__":
    main()
