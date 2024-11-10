from datetime import datetime, timezone
import logging
import os
from typing import Dict, Optional

import aiohttp
from cachetools import TTLCache
from quixplus.http_source import AuthType, HttpSource
from quixstreams import Application
from quixstreams.models import Topic


logger = logging.getLogger(__name__)


app = Application()

output_topic = Topic(
    name=os.environ.get("OUTPUT", "darkpool-trades"),
    key_serializer="str",
    value_serializer="json"
)

class DarkPoolSource(HttpSource):
    """
    Dark Pool data source implementation using UnusualWhales API.

    Features:
    - 1-second polling interval
    - TTL-based deduplication (5 minute window)
    - Authorization header handling
    - Continuous monitoring of data quality
    - Custom error handling for market data
    """

    def __init__(
        self,
        name: str,
        symbols: Optional[list[str]]=None,
        deduplication_ttl: int=300,  # 5 minute deduplication window
    ):
        # Get API token from environment
        api_token = os.getenv("UNUSUALWHALES_TOKEN")
        if not api_token:
            raise ValueError("UNUSUALWHALES_TOKEN environment variable must be set")

        # Initialize base HttpSource
        super().__init__(
            name=name,
            url="https://api.unusualwhales.com/darkpool/transactions",
            poll_interval=1.0,  # 1 second polling
            auth_type=AuthType.BEARER,
            auth_credentials=api_token,
            deduplicate=True,
            deduplication_ttl=deduplication_ttl,
            # Custom validation function
            validate=self._validate_darkpool_data,
            # Custom transformation function
            transform=self._transform_darkpool_data
        )

        self.symbols = symbols
        # Cache for tracking last known prices for validation
        self.last_prices: Dict[str, float] = {}

    def _validate_darkpool_data(self, data: Dict) -> bool:
        """
        Validates dark pool transaction data.

        Validation rules:
        1. Required fields are present
        2. Price and size are positive
        3. Price is within reasonable bounds of last known price
        4. Timestamp is recent
        """
        try:
            # Check required fields
            required_fields = {'symbol', 'exchange', 'price', 'size', 'timestamp'}
            if not all(field in data for field in required_fields):
                logger.warning(f"Missing required fields in dark pool data: {data}")
                return False

            # Validate symbol filter if specified
            if self.symbols and data['symbol'] not in self.symbols:
                return False

            # Validate price and size
            if data['price'] <= 0 or data['size'] <= 0:
                logger.warning(f"Invalid price or size in dark pool data: {data}")
                return False

            # Validate price movement (if we have a last known price)
            if data['symbol'] in self.last_prices:
                last_price = self.last_prices[data['symbol']]
                price_change_pct = abs(data['price'] - last_price) / last_price * 100
                if price_change_pct > 10:  # Alert on >10% price change
                    logger.warning(f"Large price change detected: {price_change_pct}% for {data['symbol']}")

            # Update last known price
            self.last_prices[data['symbol']] = data['price']

            # Validate timestamp (ensure not too old or future)
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            if abs((now - timestamp).total_seconds()) > 60:  # Data should be within 1 minute
                logger.warning(f"Stale or future timestamp in dark pool data: {data}")
                return False

            return True

        except Exception as e:
            logger.error(f"Error validating dark pool data: {e}")
            return False

    def _transform_darkpool_data(self, data: Dict) -> Dict:
        """
        Transforms dark pool transaction data.

        Transformations:
        1. Standardize timestamp format
        2. Add derived fields
        3. Add metadata
        """
        try:
            transformed = data.copy()

            # Standardize timestamp
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            transformed['timestamp'] = timestamp.isoformat()

            # Add derived fields
            transformed['total_value'] = data['price'] * data['size']
            transformed['source'] = 'unusualwhales'
            transformed['data_type'] = 'darkpool'

            # Add metadata
            transformed['metadata'] = {
                'source_timestamp': timestamp.timestamp(),
                'received_timestamp': datetime.now(timezone.utc).timestamp(),
                'source_name': self.name
            }

            return transformed

        except Exception as e:
            logger.error(f"Error transforming dark pool data: {e}")
            return data

    async def _get_auth_headers(self) -> Dict[str, str]:
        """Override to add any additional headers."""
        headers = await super()._get_auth_headers()
        headers.update({
            'Accept': 'application/json',
            'User-Agent': 'DarkPoolSource/1.0'
        })
        return headers

    def _handle_error(self, error: Exception):
        """Custom error handling for dark pool data."""
        if isinstance(error, aiohttp.ClientResponseError):
            if error.status == 429:  # Rate limit
                logger.error("Rate limit exceeded for UnusualWhales API")
                # Potentially implement backoff here
            elif error.status == 401:
                logger.error("Authentication failed for UnusualWhales API")
            else:
                logger.error(f"HTTP error from UnusualWhales API: {error}")
        else:
            logger.error(f"Unexpected error in dark pool source: {error}")


# Example usage
if __name__ == "__main__":
    # Create source for specific symbols
    source = DarkPoolSource(
        name="unusualwhales_darkpool",
        symbols=["SPY", "QQQ", "IWM"]  # Optional: filter specific symbols
    )

    # Create topic
    topic = Topic("darkpool_data", key_serializer="str", value_serializer="json")

    # In your Quix application:
    app.add_source(source, topic)
    # app.run()
