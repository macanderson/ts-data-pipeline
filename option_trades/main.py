"""
Option Trades
==============

Consumes: UnusualWhales API data
Produces: `option-trades`

This application consumes data from the UnusualWhales API and writes it to a Kafka topic.  It is the source of truth for option trade data in the tradesignals data pipeline.

Variables:

- **unusualwhales_token**: The API token for the UnusualWhales API.
- **output**: Name of the output topic to write to.  Defaults to `option-trades`.
"""

import logging
import os

from dotenv import load_dotenv
from quixstreams import Application
from utils import UnusualWhalesSource, extract_timestamp

# for local dev, load env vars from a .env file
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=True,
    loglevel=logging.DEBUG
)

output_topic = app.topic(
    name=os.environ["OUTPUT"],
    key_serializer='str',
    value_serializer='json',
    timestamp_extractor=extract_timestamp
)

source = UnusualWhalesSource(name=output_topic.name)
sdf = app.dataframe(source=source)
sdf.print(pretty=True)

def main():
    """Run the application"""
    app.run()

if __name__ == "__main__":
    main()
