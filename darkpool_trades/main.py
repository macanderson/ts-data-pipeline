import math
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Dict, List, TypeVar

import pytz
from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from tradesignals import Tradesignals

DarkpoolTrade = TypeVar('DarkpoolTrade')

API_KEY = 'ab40eee9-5268-4f00-aecc-518c322ed86d'

SCHEMA = """
{
  "type": "record",
  "name": "DarkpoolTrade",
  "namespace": "io.tradesignals.marketdata",
  "fields": [
    {"name": "ts", "type": "long", "doc": "Trade execution timestamp in ms since epoch."},
    {"name": "symbol", "type": "string", "doc": "Ticker symbol of the traded asset."},
    {"name": "bid", "type": "string", "doc": "Best bid price (NBBO Bid)."},
    {"name": "bid_sz", "type": "int", "doc": "Size available at the best bid price."},
    {"name": "ask", "type": "string", "doc": "Best ask price (NBBO Ask)."},
    {"name": "ask_sz", "type": "int", "doc": "Size available at the best ask price."},
    {"name": "price", "type": "string", "doc": "Traded price of the transaction."},
    {"name": "qty", "type": "int", "doc": "Size of the trade in contracts or shares."},
    {"name": "value", "type": "string", "doc": "Total trade value (price × quantity)."},
    {"name": "side", "type": {"name": "TradeSide", "type": "enum", "symbols": ["B", "S", "N"]}, "doc": "Trade side: 'B'=Buy, 'S'=Sell, 'N'=Neutral."},
    {"name": "confidence", "type": "float", "doc": "Confidence score for trade side, -1 (Sell) to +1 (Buy)."},
    {"name": "venue", "type": "string", "doc": "Trading venue where the trade was executed."}
  ]
}
"""


class DarkpoolProcessor:

    _api_key = os.environ['API_KEY']
    est_tz = pytz.timezone('US/Eastern')  # Set EST timezone

    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.client = Tradesignals(api_key=os.environ['UNUSUALWHALES_KEY'])

        # Kafka Producer Configuration
        schema_registry_conf = {
            'url': os.environ['SCHEMA_REGISTRY_URL'],
            'basic.auth.user.info': f"{os.environ['SCHEMA_REGISTRY_USERNAME']}:{os.environ['SCHEMA_REGISTRY_PASSWORD']}"
        }
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=SCHEMA
        )

        producer_conf = {
            'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
            'sasl.username': os.environ['SASL_USERNAME'],
            'sasl.password': os.environ['SASL_PASSWORD'],
            'sasl.mechanisms': os.environ['SASL_MECHANISM'],
            'security.protocol': 'SASL_SSL',
        }
        self.producer = Producer(producer_conf)

    @staticmethod
    def calculate_side(price: Decimal, bid: Decimal, ask: Decimal) -> str:
        """Calculate the side."""
        if abs(price - bid) < abs(price - ask):
            return 'S'
        elif abs(price - bid) > abs(price - ask):
            return 'B'
        return 'N'

    @staticmethod
    def calculate_confidence_score(price: Decimal, bid: Decimal, ask: Decimal) -> float:
        """Calculate the side confidence."""
        mid_price: Decimal = (bid + ask) / 2
        normalized_distance: Decimal = (price - mid_price) / (ask - bid)
        confidence: float = 1 / (1 + math.exp(-float(normalized_distance)))  # Sigmoid function
        return confidence * 2 - 1  # Map sigmoid output to range [-1, 1]

    @staticmethod
    def generate_key(trade: Dict) -> str:
        """Generate hash for the message key."""
        key_data = f"{trade['symbol']}{trade['ts']}{trade['price']}{trade['qty']}"
        return sha256(key_data.encode('utf-8')).hexdigest()

    def map_trade_to_dict(self, trade: DarkpoolTrade) -> Dict:
        """Map vendor schema to the Tradesignals schema."""
        executed_at_est: datetime = trade.executed_at.astimezone(DarkpoolProcessor.est_tz)
        price = Decimal(trade.price)
        bid = Decimal(trade.nbbo_bid)
        ask = Decimal(trade.nbbo_ask)
        side = DarkpoolProcessor.calculate_side(price, bid, ask)
        confidence = DarkpoolProcessor.calculate_confidence_score(price, bid, ask)

        # Build tags list
        tags = []
        if trade.ext_hour_sold_codes is not None:
            tags.append(f"condition:{trade.ext_hour_sold_codes}")
        if trade.sale_cond_codes is not None:
            tags.append(f"condition:{trade.sale_cond_codes}")
        if trade.trade_settlement is not None:
            tags.append(f"condition:{trade.trade_settlement}")

        if side == "N":
            tags.append('sentiment:neutral')
        elif side == "B":
            tags.append('sentiment:bullish')
        elif side == "S":
            tags.append('sentiment:bearish')

        return {
            'ts': int(trade.executed_at.timestamp() * 1000),  # Convert to milliseconds since epoch
            'symbol': trade.ticker,
            'bid': str(trade.nbbo_bid),
            'bid_sz': trade.nbbo_bid_quantity,
            'ask': str(trade.nbbo_bid),
            'ask_sz': trade.nbbo_ask_quantity,
            'price': str(trade.price),
            'qty': trade.size,
            'value': str(trade.premium),
            'side': side,
            'bull_bear': confidence,
            'venue': trade.market_center,
            'tags': tags,
        }

    def produce_to_kafka(self, trade: Dict) -> None:
        try:
            key = self.generate_key(trade)
            value = self.avro_serializer(
                trade,
                SerializationContext(topic=os.environ['KAFKA_TOPIC'], field=MessageField.VALUE)
            )
            self.producer.produce(
                topic=os.environ['KAFKA_TOPIC'],
                key=key,
                value=value,
            )
            self.producer.flush()
            print(f"Produced to Kafka: {trade}")
        except KafkaException as e:
            print(f"Kafka Error: {e}")

    def process_trades_for_symbol(self, symbol: str, trades: List[DarkpoolTrade]) -> None:
        for trade in trades:
            mapped_trade = self.map_trade_to_dict(trade)
            self.produce_to_kafka(mapped_trade)

    def fetch_and_process_trades(self) -> None:
        now_ts: datetime = datetime.now(timezone.utc)
        start_ts: datetime = now_ts - timedelta(days=90)

        for symbol in self.symbols:
            print(f"Processing trades for symbol: {symbol}")
            newer_than_ts: datetime = start_ts

            while True:
                trades = self.client.darkpool.trades_by_ticker.list(
                    ticker=symbol,
                    limit=500,
                    newer_than=newer_than_ts.isoformat()
                )

                if not trades:
                    print(f"No more trades for {symbol}")
                    break

                self.process_trades_for_symbol(symbol, trades)

                newest_trade_time = max(trades, key=lambda t: t.executed_at).executed_at
                newer_than_ts = newest_trade_time + timedelta(milliseconds=1)

    def run(self) -> None:
        """Run the processor."""
        self.fetch_and_process_trades()


if __name__ == "__main__":
    symbols = ["NVDA", "AAPL", "MSFT"]
    processor = DarkpoolProcessor(symbols)
    processor.run()
