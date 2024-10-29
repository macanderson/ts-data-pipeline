from datetime import timedelta
import os
from typing import Any, List, Optional, Tuple

import numpy as np
import pandas as pd

from quixstreams import Application  # import the Quix Streams modules for interacting with Kafka
from quixstreams.models import TimestampType

# initialize application
app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=True,
    auto_offset_reset="earliest",
    consumer_group="option-trade-aggs",
)

# define custom timestamp extractor for windowed aggregations
def extract_timestamp(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp in the message.
    """
    return value["ts"]

# configure the input topic
input_topic = app.topic(
    os.environ.get("INPUT", "default_input_topic"),
    timestamp_extractor=extract_timestamp,
    key_deserializer="str",
    value_deserializer="json",
)

# configure the output topic
output_topic = app.topic(
    os.environ.get("OUTPUT", "default_output_topic"),
    key_serializer="str",
    value_serializer="json",
)

def main():
    """
    Main function to aggregate option trades by option symbol.
    """
    try:
        # Process incoming data using Streaming DataFrame
        sdf = app.dataframe(input_topic)

        sdf = sdf.group_by("osym")


        # tumbling window, but emitting results for each incoming message
        sdf = (
            sdf.tumbling_window(duration_ms=timedelta(hours=1))
            .sum()
            .current()
        )
        # Print incoming data
        sdf.print()

        # Produce data to the output_topic topic
        sdf = sdf.to_topic(output_topic)

        # Run the app
        app.run(sdf)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        app.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
