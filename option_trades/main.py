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

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
from quixstreams import Application
from utils import UnusualWhalesSource, extract_timestamp

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Initialize the application
app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=True
)

# Define the Kafka topics
output_topic = app.topic(
    name=os.environ["OUTPUT"],
    key_serializer='str',
    value_serializer='json',
    key_deserializer='str',
    value_deserializer='json',
    timestamp_extractor=extract_timestamp
)

source = UnusualWhalesSource(name="option-trades")
sdf = app.dataframe(source=source, topic=output_topic)

# put transformation logic here
# see docs for what you can do
# https://quix.io/docs/get-started/quixtour/process-threshold.html

sdf.print()
sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)