"""
Quix application helper functions..
"""
import os
from typing import Callable

from dotenv import load_dotenv
from quixstreams.application import Application
from quixstreams.kafka.configuration import ConnectionConfig

load_dotenv()

logger = logging.getLogger(__name__)

def create_app(
    consumer_group: str = "default-consumer-group",
    auto_offset_reset: str = "latest",
    on_message_processed: Callable | None = None,
    loglevel: str = "INFO",
):
    """
    Initialize a Quix application.

    Required environment variables:
        BOOTSTRAP_SERVERS
        SASL_USERNAME
        SASL_PASSWORD
        SASL_MECHANISM
        SECURITY_PROTOCOL

    Args:
        consumer_group: The consumer group to use for the application.
        auto_offset_reset: The auto offset reset to use for the application.
        on_message_processed: A callback function that is called when a
                            message is processed.
        loglevel: The log level to use for the application. Defaults to "INFO".
    """
    logger.setLevel(loglevel)
    logger.info("Creating application")
    connection = ConnectionConfig(
        broker_address=os.environ["BOOTSTRAP_SERVERS"],
        username=os.environ["SASL_USERNAME"],
        password=os.environ["SASL_PASSWORD"],
        sasl_mechanism=os.environ["SASL_MECHANISM"],
        security_protocol=os.environ["SECURITY_PROTOCOL"],
    )

    return Application(
        broker_address=connection,
        consumer_group=consumer_group,
        auto_offset_reset=auto_offset_reset,
        on_message_processed=on_message_processed,
        logger=logger,
    )
