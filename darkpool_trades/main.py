import os
from datetime import datetime
from typing import Dict, List

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.models import Topic
from quixstreams.sources.base.source import Source
from unusualwhales import Unusualwhales

load_dotenv()

class DarkpoolTradesSource(Source):
    """
    Source for fetching darkpool trades from UnusualWhales API.
    """

    def __init__(self, name: str):
        """
        Initialize the DarkpoolTradesSource with a name and UnusualWhales client.

        Args:
            name (str): The name of the source.
        """
        super().__init__(name)
        self.client = Unusualwhales(api_key=os.environ["UNUSUALWHALES_TOKEN"])
        self.last_polled = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    def poll_darkpool_trades(self) -> None:
        """
        Poll the UnusualWhales API for the latest darkpool trades and produce messages to the output topic.
        """
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
        while self.running:
            for trade in self.client.get_darkpool_trades():
                print(trade)


if __name__ == "__main__":
    app = Application(broker_address="localhost:9092")
    source = DarkpoolTradesSource(name="darkpool_trades")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()
