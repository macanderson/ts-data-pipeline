import logging
import os

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.community.iceberg import IcebergSink, AWSIcebergConfig


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

def on_consumer_error(error):
    logger.info(f"Consumer error: {error}")
    
app = Application(
    consumer_group="iceberg-sink", 
    auto_offset_reset = "earliest",
    commit_interval=5,
    on_consumer_error=on_consumer_error,
)

input_topic = app.topic(os.environ["INPUT"])

print(f"Using input topic: {input_topic} to sink to s3 iceberg table `{os.environ['TABLE_NAME']}`")
iceberg_sink = IcebergSink(
    data_catalog_spec="aws_glue",
    table_name=os.environ["TABLE_NAME"],
    config=AWSIcebergConfig(
        aws_s3_uri=os.environ["AWS_S3_URI"],
        aws_region=os.environ["AWS_REGION"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"]))

sdf = app.dataframe(input_topic)

sdf.sink(iceberg_sink)

if __name__ == "__main__":
    app.run(sdf)
