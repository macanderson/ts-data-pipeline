import os
from typing import Dict, List

from dotenv import load_dotenv
from quixstreams import Application
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
