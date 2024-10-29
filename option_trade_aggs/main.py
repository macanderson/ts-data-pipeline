from datetime import timedelta
import logging
import os

from quixstreams import Application  # import the Quix Streams modules for interacting with Kafka

from utils import extract_timestamp

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


app = Application(
    broker_address=None,
    processing_guarantee="exactly-once",
    auto_create_topics=True,
    auto_offset_reset="earliest",
    consumer_group="option-trade-aggs",
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

    It will prime the aggregation when the first record arrives 
    in the window.
    """
    initialized_obj = {
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
        sdf = app.dataframe(input_topic)
        sdf = sdf.group_by("osym")
        sdf = (
            sdf.tumbling_window(timedelta(minutes=1))
            .reduce(reducer=reducer, initializer=initializer)
            .current()
        )
        sdf = sdf.to_topic(output_topic)
        app.run(sdf)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        app.clear_state()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
