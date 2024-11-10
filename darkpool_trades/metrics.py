import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Set, Union

import pandas as pd
from quixstreams.models import Topic
from quixstreams.sources.base.source import Source


logger = logging.getLogger(__name__)


@dataclass
class DataQualityMetrics:
    """Tracks data quality metrics for market data."""
    timestamp: datetime
    message_count: int
    error_count: int
    latency_ms: float
    missing_fields: Set[str]
    stale_data_count: int
    price_gaps: int
    zero_prices: int


class MarketDataMonitor:
    """
    Enhanced monitoring system for market data sources.

    Features:
    - Real-time data quality metrics
    - Latency tracking
    - Price continuity checking
    - Data freshness monitoring
    - Alert generation
    - Historical metrics storage
    """

    def __init__(
        self,
        source: Source,
        topic: Topic,
        required_fields: Set[str],
        max_latency_ms: float=1000.0,
        max_price_gap_percent: float=5.0,
        metrics_window_size: int=1000,
        alert_threshold: int=3
    ):
        self.source = source
        self.topic = topic
        self.required_fields = required_fields
        self.max_latency_ms = max_latency_ms
        self.max_price_gap_percent = max_price_gap_percent
        self.alert_threshold = alert_threshold

        # Metrics storage
        self.metrics_history = deque(maxlen=metrics_window_size)
        self.last_price: Optional[float] = None
        self.last_update_time: Optional[datetime] = None
        self.consecutive_alerts = 0

        # Initialize monitoring
        self._setup_monitoring()

    def _setup_monitoring(self):
        """Set up monitoring hooks on the source."""
        original_produce = self.source.produce

        def monitored_produce(key: Any, value: Dict, headers: Dict=None, timestamp: int=None):
            """Wrapper around produce to monitor data quality."""
            try:
                metrics = self._check_message_quality(key, value, headers, timestamp)
                self.metrics_history.append(metrics)

                if self._should_alert(metrics):
                    self._handle_alert(metrics)
                else:
                    self.consecutive_alerts = 0

                original_produce(key, value, headers, timestamp)

            except Exception as e:
                logger.error(f"Error in message monitoring: {e}")
                self._handle_error(e)

        self.source.produce = monitored_produce

    def _check_message_quality(
        self,
        key: Any,
        value: Dict,
        headers: Dict,
        timestamp: int
    ) -> DataQualityMetrics:
        """Check quality metrics for a single message."""
        now = datetime.utcnow()

        # Calculate basic metrics
        missing_fields = self.required_fields - set(value.keys())
        message_latency = (now - datetime.fromtimestamp(timestamp / 1000)).total_seconds() * 1000

        # Check price continuity
        price_gaps = 0
        zero_prices = 0
        if 'price' in value:
            current_price = float(value['price'])
            if self.last_price:
                price_change_percent = abs(current_price - self.last_price) / self.last_price * 100
                if price_change_percent > self.max_price_gap_percent:
                    price_gaps += 1
            if current_price == 0:
                zero_prices += 1
            self.last_price = current_price

        # Check data freshness
        stale_data_count = 0
        if self.last_update_time:
            time_since_last = (now - self.last_update_time).total_seconds() * 1000
            if time_since_last > self.max_latency_ms:
                stale_data_count += 1
        self.last_update_time = now

        return DataQualityMetrics(
            timestamp=now,
            message_count=1,
            error_count=0,
            latency_ms=message_latency,
            missing_fields=missing_fields,
            stale_data_count=stale_data_count,
            price_gaps=price_gaps,
            zero_prices=zero_prices
        )

    def _should_alert(self, metrics: DataQualityMetrics) -> bool:
        """Determine if metrics warrant an alert."""
        return (
            metrics.latency_ms > self.max_latency_ms or
            len(metrics.missing_fields) > 0 or
            metrics.price_gaps > 0 or
            metrics.zero_prices > 0 or
            metrics.stale_data_count > 0
        )

    def _handle_alert(self, metrics: DataQualityMetrics):
        """Handle alert conditions."""
        self.consecutive_alerts += 1

        if self.consecutive_alerts >= self.alert_threshold:
            alert_msg = (
                f"MARKET DATA QUALITY ALERT:\n"
                f"Source: {self.source.name}\n"
                f"Topic: {self.topic.name}\n"
                f"Latency: {metrics.latency_ms:.2f}ms\n"
                f"Missing Fields: {metrics.missing_fields}\n"
                f"Price Gaps: {metrics.price_gaps}\n"
                f"Zero Prices: {metrics.zero_prices}\n"
                f"Stale Data Count: {metrics.stale_data_count}"
            )
            logger.warning(alert_msg)
            # Here you could add additional alert channels (email, Slack, etc.)

    def _handle_error(self, error: Exception):
        """Handle monitoring errors."""
        logger.error(f"Market data monitoring error: {error}")
        # Here you could add additional error handling logic

    def get_metrics_summary(self) -> pd.DataFrame:
        """Get a summary of recent metrics as a DataFrame."""
        return pd.DataFrame([
            {
                'timestamp': m.timestamp,
                'message_count': m.message_count,
                'error_count': m.error_count,
                'latency_ms': m.latency_ms,
                'missing_fields_count': len(m.missing_fields),
                'stale_data_count': m.stale_data_count,
                'price_gaps': m.price_gaps,
                'zero_prices': m.zero_prices
            }
            for m in self.metrics_history
        ])


# Example usage
if __name__ == "__main__":
    from http_source import HttpSource

    # Create source and topic
    source = HttpSource(
        name="crypto_prices",
        url="https://api.exchange.com/v1/prices",
        poll_interval=1.0
    )
    topic = Topic("market_data")

    # Initialize monitor
    monitor = MarketDataMonitor(
        source=source,
        topic=topic,
        required_fields={'price', 'timestamp', 'symbol'},
        max_latency_ms=500.0,
        max_price_gap_percent=3.0
    )

    # Later, you can get metrics
    metrics_df = monitor.get_metrics_summary()
    print(metrics_df.describe())
