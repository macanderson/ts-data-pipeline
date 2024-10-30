from datetime import timedelta
import logging
import os

from dotenv import load_dotenv
from quixstreams import (
    Application
)
from quixstreams.kafka.configuration import ConnectionConfig

from utils import extract_timestamp


load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

print("File name:")
print(__file__)

connection = ConnectionConfig(
    bootstrap_servers=os.environ["KAFKA_BROKER_ADDRESS"],
    sasl_mechanism=os.environ["KAFKA_SASL_MECHANISM"],
    security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
    sasl_username=os.environ["KAFKA_USERNAME"],
    sasl_password=os.environ["KAFKA_PASSWORD"],
)


def on_message_processed(topic:str, partition: int, offset: int):
    """
    Callback function that is called when a message is processed.
    """
    print(f"Message processed: {topic}, {partition}, {offset}")


app = Application(
    broker_address=connection,
    processing_guarantee="exactly-once",
    auto_create_topics=True,
    auto_offset_reset="earliest",
    consumer_group="option_trade_aggs",
    use_changelog_topics=True,
    on_message_processed=on_message_processed,
)


input_topic = app.topic(
    os.environ.get("INPUT", "option-trades"),
    timestamp_extractor=extract_timestamp,
    key_deserializer="str",
    value_deserializer="json",
)


output_topic = app.topic(
    os.environ.get("OUTPUT", "option-trade-aggs"),
    key_serializer="str",
    value_serializer="json",
)


def reducer(aggregated: dict, value: dict) -> dict:
    """
    Calculate "min", "max", "total" and "average" over temperature values.

    Reducer always receives two arguments:
    - previously aggregated value (the "aggregated" argument)
    - current value (the "value" argument)
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    if aggregated is None:
        aggregated = initializer(None)
    aggregated['count'] += 1
    if value['premium'] > 250000:
        if value['side'] == 'buy' and value['otype'] == 'put':
            aggregated['whale_buy_put_vol'] += value['qty']
            aggregated['whale_buy_put_prem'] += value['premium']
        elif value['side'] == 'sell' and value['otype'] == 'put':
            aggregated['whale_sell_put_vol'] += value['qty']
            aggregated['whale_sell_put_prem'] += value['premium']
        elif value['side'] == 'buy' and value['otype'] == 'put':
            aggregated['whale_nsd_put_vol'] += value['qty']
            aggregated['whale_nsd_put_prem'] += value['premium']
        elif value['side'] == 'buy' and value['otype'] == 'call':
            aggregated['whale_buy_call_vol'] += value['qty']
            aggregated['whale_buy_call_prem'] += value['premium']
        elif value['side'] == 'sell' and value['otype'] == 'call':
            aggregated['whale_sell_call_vol'] += value['qty']
            aggregated['whale_sell_call_prem'] += value['premium']
        else:
            aggregated['whale_nsd_call_vol'] += value['qty']
            aggregated['whale_nsd_call_prem'] += value['premium']
    else:
        if value['side'] == 'buy' and value['otype'] == 'put':
            aggregated['buy_put_vol'] += value['qty']
            aggregated['buy_put_prem'] += value['premium']
        elif value['side'] == 'sell' and value['otype'] == 'put':
            aggregated['sell_put_vol'] += value['qty']
            aggregated['sell_put_prem'] += value['premium']
        elif value['side'] == 'buy' and value['otype'] == 'put':
            aggregated['nsd_put_vol'] += value['qty']
            aggregated['nsd_put_prem'] += value['premium']
        elif value['side'] == 'buy' and value['otype'] == 'call':
            aggregated['buy_call_vol'] += value['qty']
            aggregated['buy_call_prem'] += value['premium']
        elif value['side'] == 'sell' and value['otype'] == 'call':
            aggregated['sell_call_vol'] += value['qty']
            aggregated['sell_call_prem'] += value['premium']
        else:
            aggregated['nsd_call_vol'] += value['qty']
            aggregated['nsd_call_prem'] += value['premium']

    return aggregated


def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.

    It will prime the aggregation when the first record arrpythonives
    in the window.
    """
    if value is None:
        value = {}
    initialized_obj = {
        'osym': value.get('osym', ''),
        'usym': value.get('usym', ''),
        'strike': value.get('strike', 0),
        'expiry': value.get('expiry', ''),
        'otype': value.get('otype', ''),
        'dtx': value.get('dtx', 0),
        'count': 0,
        'whale_buy_put_vol': 0,
        'whale_buy_put_prem': 0,
        'whale_sell_put_vol': 0,
        'whale_sell_put_prem': 0,
        'whale_nsd_put_vol': 0,
        'whale_nsd_put_prem': 0,
        'whale_buy_call_vol': 0,
        'whale_buy_call_prem': 0,
        'whale_sell_call_vol': 0,
        'whale_sell_call_prem': 0,
        'whale_nsd_call_vol': 0,
        'whale_nsd_call_prem': 0,
        'buy_put_vol': 0,
        'buy_put_prem': 0,
        'sell_put_vol': 0,
        'sell_put_prem': 0,
        'nsd_put_vol': 0,
        'nsd_put_prem': 0,
        'buy_call_vol': 0,
        'buy_call_prem': 0,
        'sell_call_vol': 0,
        'sell_call_prem': 0,
        'nsd_call_vol': 0,
        'nsd_call_prem': 0,
    }
    return reducer(initialized_obj, value)


def main():
    """
    Main function to aggregate option trades by option symbol.
    """
    logger.info("Starting the application to aggregate option trade data.")
    try:
        print("Starting the application to aggregate option trade data.")
        sdf = app.dataframe(input_topic)
        print("Dataframe created.")
        sdf["premium"] = sdf["price"] + sdf["qty"]
        print("Grouped by osym.")
        sdf = (
            sdf.tumbling_window(timedelta(minutes=1), grace_ms=1000)
            .reduce(reducer=reducer, initializer=initializer)
            .current()
        )
        sdf.print()
        print("Reduced.")
        sdf = sdf.to_topic(output_topic)
        print("Dataframe written to output topic.")
        app.run()
        print("Application running.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        app.clear_state()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
