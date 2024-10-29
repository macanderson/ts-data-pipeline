"""Utils for the option-trade-aggs application."""
from typing import Any, List, Optional, Tuple

from quixstreams.models import TimestampType


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

