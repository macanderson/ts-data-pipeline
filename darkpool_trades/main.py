import os

from quixstreams import \
    Application  # import the Quix Streams modules for interacting with Kafka

app = Application(auto_create_topics=False)  # create an Application

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

    # Process incoming data using Streaming DataFrame
    # sdf = app.dataframe(input_topic)

    # Print incoming data
    # sdf.print()

    # Produce data to the output topic
    # sdf = sdf.to_topic(OUTPUT)

    # Run the app
    # app.run(sdf)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
