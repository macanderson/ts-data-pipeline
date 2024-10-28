import os
from datetime import timedelta

from quixstreams import (
    Application,  # import the Quix Streams modules for interacting with Kafka
)

app = Application()  # create an Application

# input - InputTopic
input = app.topic(os.environ["input"])

# output - OutputTopic
output = app.topic(os.environ["output"])


def main():
    """
    Your code goes here.
    See the Quix Streams documentation for more examples:
    https://quix.io/docs/quix-streams/quickstart.html
    """

    # Process incoming data using Streaming DataFrame
    sdf = app.dataframe(input)

    sdf = sdf.group_by("osym")
    sdf = (
        sdf.tumbling_window(
            timedelta(minutes=1),
        )
        .reduce(
            reducer=reducer,
            initializer=initializer,
        )
        .final()
    )

    # Print incoming data
    # sdf.print()

    # Produce data to the output topic
    # sdf = sdf.to_topic(output)

    # Run the app
    # app.run(sdf)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
