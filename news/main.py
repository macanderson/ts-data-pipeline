import os

from dotenv import load_dotenv
from quixstreams import Application
from quixplus import HttpSource

load_dotenv()


app = Application(
    broker_address=None,
    processing_guarantee="at-least-once",
    auto_create_topics=False,
)

output = app.topic(
    os.environ.get("OUTPUT", "news"),
    key_serializer="str",
    value_serializer="json"
)


source = app.source(
    name="news",
    key_deserializer="str",
    value_deserializer="json"
)

def main():


    app.add_source(source)

    sdf = app.dataframe(source=source)
    sdf.print(pretty=True)
    app.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
