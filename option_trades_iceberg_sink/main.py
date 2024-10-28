import os

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.community.iceberg import AWSIcebergConfig, IcebergSink

load_dotenv()

app = Application(consumer_group="option_trades_iceberg_sink",
                  auto_offset_reset = "earliest",
                  commit_interval=5)

input_topic = app.topic(os.environ["INPUT"])

iceberg_sink = IcebergSink(
    data_catalog_spec="aws_glue",
    OPTION_TRADES_TABLE_NAME=os.environ["OPTION_TRADES_TABLE_NAME"],
    config=AWSIcebergConfig(
        aws_s3_uri=os.environ["AWS_S3_ICEBERG_URI"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"]))

sdf = app.dataframe(input_topic)
sdf.sink(iceberg_sink)

if __name__ == "__main__":
    app.run(sdf)
