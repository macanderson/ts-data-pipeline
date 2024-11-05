from quixstreams import Application  # import the Quix Streams modules for interacting with Kafka
import os

app = Application()  # create an Application

# INPUT - InputTopic
INPUT = app.topic(os.environ["INPUT"])

# news - OutputTopic
news = app.topic(os.environ["news"])


def main():
    """
    Your code goes here.
    See the Quix Streams documentation for more examples:
    https://quix.io/docs/quix-streams/quickstart.html
    """

    # Process incoming data using Streaming DataFrame
    # sdf = app.dataframe(INPUT)

    # Print incoming data
    # sdf.print()

    # Produce data to the output topic
    # sdf = sdf.to_topic(news)

    # Run the app
    # app.run(sdf)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
