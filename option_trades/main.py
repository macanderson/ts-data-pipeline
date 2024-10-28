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

WHALE_PREMIUM_THRESHOLD = os.environ.get("WHALE_PREMIUM_THRESHOLD", 100000)

def on_message_processed(message):
    logger.info(f"Message processed: {message}")

def on_message_dropped(message):
    logger.error(f"Message dropped: {message}")

def on_processing_error(error):
    logger.error(f"Processing error: {error}")

def on_producer_error(error):    
    logger.error(f"Producer error: {error}")
    

# Initialize the application
app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=True,
    auto_offset_reset="latest",
    consumer_group="option-trades",
    on_message_processed=on_message_processed,
    on_message_dropped=on_message_dropped,
    on_processing_error=on_processing_error,
    on_producer_error=on_producer_error,
    loglevel=logging.DEBUG,
)

# Define the Kafka topics
output_topic = app.topic(
    name=os.environ["OUTPUT"],
    key_serializer='str',
    value_serializer='json',
    timestamp_extractor=extract_timestamp
)

source = UnusualWhalesSource(name=os.environ["OUTPUT"])
sdf = app.dataframe(source=source, topic=output_topic)
sdf["premium"] = sdf["price"] * sdf["qty"]
sdf["whale_trade"] = sdf["premium"] > WHALE_PREMIUM_THRESHOLD
sdf.print()
sdf.to_topic(output_topic)

def main():
    """Run the application"""
    app.run(sdf)

if __name__ == "__main__":
    main()
