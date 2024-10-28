from quixstreams import Application  # import the Quix Streams modules for interacting with Kafka
import os

app = Application()  # create an Application

# INPUT - InputTopic
INPUT = app.topic(os.environ["INPUT"])

# OUTPUT - OutputTopic
OUTPUT = app.topic(os.environ["OUTPUT"])

# MODEL_NAME - FreeText
MODEL_NAME = os.environ["MODEL_NAME"]


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
    # sdf = sdf.to_topic(OUTPUT)
    
    # Run the app
    # app.run(sdf)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
