import os

from quixplus import HttpSource
from quixstreams import \
    Application  # import the Quix Streams modules for interacting with Kafka

app = Application(auto_create_topics=False, consumer_group="Tradesignals-Data-Pipeline")  # create an Application

# OUTPUT - OutputTopic
OUTPUT = app.topic(os.environ["OUTPUT"], key_serializer="str", value_serializer="json")

# UNUSUALWHALES_TOKEN - Secret
UNUSUALWHALES_TOKEN = os.environ["UNUSUALWHALES_TOKEN"]


# input_topic = app.topic("input-topic-name")

def main():
    """
    Your code goes here.
    See the Quix Streams documentation for more examples:
    https://quix.io/docs/quix-streams/quickstart.html
    """

    source = HttpSource(
        name="darkpool-trades",
        url="https://api.unusualwhales.com/api/v1/option_trade",
        headers={"Authorization": f"Bearer {UNUSUALWHALES_TOKEN}"},
        poll_interval=1,
        key_serializer="str",
        value_serializer="json",
        key_json_path="symbol"
    )

    app = Application(
        broker_address=None,
        auto_create_topics=False,
        consumer_group="darkpool-trades",
        processing_guarantee="exactly-once"
    )

    output_topic = app.topic(
        name="darkpool-trades",
        key_serializer="str",
        value_serializer="json"
    )

    app.add_source(source=source, topic=output_topic)

    sdf = app.dataframe(output_topic)
    sdf.print(pretty=True)

    app.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
