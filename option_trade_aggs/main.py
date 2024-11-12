"""
This script reads option trade data from a Kafka topic, aggregates the data
based on the trade premium value and trade volume, and writes the aggregated data to another
Kafka topic.
"""
import logging
import os
import sys
import time
from datetime import timedelta
from functools import wraps
from typing import Any, List, Optional, Tuple

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.models import TimestampType

load_dotenv()

def extract_timestamp(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp in the message.
    """
    return value["ts"]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def retry_on_exception(retries=3, delay=1):
    """Decorator to retry functions on exception"""

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries - 1:  # Last retry
                        logger.error(f"Failed after {retries} retries: {str(e)}")
                        raise
                    logger.warning(f"Attempt {i + 1} failed: {str(e)}. Retrying...")
                    time.sleep(delay * (i + 1))  # Exponential backoff
            return None

        return wrapper

    return decorator


def safe_get_env(key: str, default: Optional[str]=None) -> str:
    """Safely get environment variables with logging"""
    value = os.environ.get(key, default)
    if value is None:
        logger.error(f"Required environment variable {key} is not set")
        sys.exit(1)
    return value


@retry_on_exception(retries=3)
def initialize_app() -> Application:
    """Initialize the Quix application with retries"""
    return Application(
        broker_address=None,
        processing_guarantee="exactly-once",
        auto_create_topics=False,
        auto_offset_reset="latest",
        consumer_group="option_trade_aggs",
        use_changelog_topics=True,
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
    try:
        if aggregated is None:
            aggregated = initializer(None)
        aggregated['count'] += 1
        if value['premium'] > 250000:
            if value.get("side", "") == 'buy' and value['otype'] == 'put':
                aggregated['whale_bought_put_vol'] += value['qty']
                aggregated['whale_bought_put_prem'] += value['premium']
            elif value['side'] == 'sell' and value['otype'] == 'put':
                aggregated['whale_sold_put_vol'] += value['qty']
                aggregated['whale_sold_put_prem'] += value['premium']
            elif value['otype'] == 'put':
                aggregated['whale_no_side_put_vol'] += value['qty']
                aggregated['whale_no_side_put_prem'] += value['premium']
            elif value['side'] == 'buy' and value['otype'] == 'call':
                aggregated['whale_bought_call_vol'] += value['qty']
                aggregated['whale_bought_call_prem'] += value['premium']
            elif value['side'] == 'sell' and value['otype'] == 'call':
                aggregated['whale_sold_call_vol'] += value['qty']
                aggregated['whale_sold_call_prem'] += value['premium']
            else:
                aggregated['whale_no_side_call_vol'] += value['qty']
                aggregated['whale_no_side_call_prem'] += value['premium']
        else:
            if value['side'] == 'buy' and value['otype'] == 'put':
                aggregated['bought_put_vol'] += value['qty']
                aggregated['bought_put_prem'] += value['premium']
            elif value['side'] == 'sell' and value['otype'] == 'put':
                aggregated['sold_put_vol'] += value['qty']
                aggregated['sold_put_prem'] += value['premium']
            elif value['otype'] == 'put':
                aggregated['no_side_put_vol'] += value['qty']
                aggregated['no_side_put_prem'] += value['premium']
            elif value['side'] == 'buy' and value['otype'] == 'call':
                aggregated['bought_call_vol'] += value['qty']
                aggregated['bought_call_prem'] += value['premium']
            elif value['side'] == 'sell' and value['otype'] == 'call':
                aggregated['sold_call_vol'] += value['qty']
                aggregated['sold_call_prem'] += value['premium']
            else:
                aggregated['no_side_call_vol'] += value['qty']
                aggregated['no_side_call_prem'] += value['premium']
        return aggregated
    except Exception as e:
        logger.error(f"Error in reducer: {str(e)}")
        return aggregated


def initializer(value: dict | None = None) -> dict:
    """
    Initialize the state for aggregation when a new window starts.

    It will prime the aggregation when the first record arrpythonives
    in the window.

    Args:
        value: The initial value to use for the aggregation.
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
        'whale_bought_put_vol': 0,
        'whale_bought_put_prem': 0,
        'whale_sold_put_vol': 0,
        'whale_sold_put_prem': 0,
        'whale_no_side_put_vol': 0,
        'whale_no_side_put_prem': 0,
        'whale_bought_call_vol': 0,
        'whale_bought_call_prem': 0,
        'whale_sold_call_vol': 0,
        'whale_sold_call_prem': 0,
        'whale_no_side_call_vol': 0,
        'whale_no_side_call_prem': 0,
        'bought_put_vol': 0,
        'bought_put_prem': 0,
        'sold_put_vol': 0,
        'sold_put_prem': 0,
        'no_side_put_vol': 0,
        'no_side_put_prem': 0,
        'bought_call_vol': 0,
        'bought_call_prem': 0,
        'sold_call_vol': 0,
        'sold_call_prem': 0,
        'no_side_call_vol': 0,
        'no_side_call_prem': 0,
    }
    return reducer(initialized_obj, value)


@retry_on_exception(retries=3)
def main():
    """Main function with improved error handling and retries"""
    logger.info("Starting the application to aggregate option trade data.")
    app = None

    try:
        app = initialize_app()

        input_topic_name = safe_get_env("INPUT", "option-trades")
        output_topic_name = safe_get_env("OUTPUT", "option-trade-aggs")

        input_topic = app.topic(
            input_topic_name,
            timestamp_extractor=extract_timestamp,
            key_deserializer="str",
            value_deserializer="json",
        )

        output_topic = app.topic(
            output_topic_name,
            key_serializer="str",
            value_serializer="json",
        )

        sdf = app.dataframe(input_topic)
        logger.info("Dataframe created successfully")

        # Apply tumbling window and reduce
        # emit the final result only once per window
        sdf = (
            sdf.tumbling_window(timedelta(minutes=1), grace_ms=1000)
            .reduce(reducer=reducer, initializer=initializer)
            .final()
        )

        sdf.to_topic(output_topic)
        logger.info("Dataframe written to output topic")

        app.run()
        logger.info("Application running")

    except KeyboardInterrupt:
        logger.info("Received shutdown signal, cleaning up...")
    except Exception as e:
        logger.error(f"Critical error: {str(e)}", exc_info=True)
        raise
    finally:
        if app:
            try:
                app.clear_state()
                logger.info("Application state cleared")
            except Exception as e:
                logger.error(f"Error clearing application state: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Application failed to start %s", e, exc_info=True)
        sys.exit(1)
