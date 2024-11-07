"""Utility functions."""
from datetime import datetime
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import websockets
from websockets.sync.client import connect

from data_source import CustomSource
from quixstreams.models import TimestampType, Topic


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def add_field(key:str, value: Any, record: dict | None = None) -> dict:
    """Add a field to a dictionary."""
    if record is None:
        record = {}
    record[key] = value
    return record


def extract_timestamp(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    """Extract the timestamp from the message."""
    return value.get("ts") or 0


def map_fields(data: Dict[Any, Any]) -> dict:
    """Map option data to a dictionary."""
    try:
        tags = data.get('tags', [])
        flags = data.get('report_flags', [])
        tags.extend(flags)

        position_type = "no_side_"
        if "ask_side" in tags:
            position_type = "long_"
        elif "bid_side" in tags:
            position_type = "short_"
        elif "no_side" in tags:
            position_type = "neutral_"
        position_type += data.get('option_type')

        if (float(data.get("premium")) > 75000) and float(data.get("premium")):  # noqa E501
            tags.append("large_trade")

        if float(data.get("premium")) > 250000 and float(data.get("premium")) < 1000000:
            tags.append("whale")
        elif float(data.get("premium")) > 1000000:
            tags.append("millionaire")
        tags.append(position_type)
        todays_date = datetime.fromtimestamp(data.get('executed_at', 0) / 1000).date()
        expiry = datetime.fromisoformat(data.get('expiry', '1800-01-01')).date()
        days_to_expiry = (expiry - todays_date).days

        if days_to_expiry <= 0:
            tags.append("expires_today")
        elif days_to_expiry <= 7:
            tags.append("expires_soon")

        result = {
            'id': data.get('id'),
            'ts': data.get('executed_at', 0),
            'osym': data.get('option_symbol'),
            'usym': data.get('underlying_symbol'),
            'spot': float(data.get('underlying_price', '0') or '0'),
            'strike': float(data.get('strike', '0') or '0'),
            'expiration': data.get('expiry'),
            'dtx': days_to_expiry,
            'otype': data.get('option_type'),
            'qty': data.get('size', 0),
            'price': float(data.get('price', '0') or '0'),
            'premium': float(data.get('premium', '0') or '0'),
            'xchg': data.get('exchange', ''),
            'cond': data.get('trade_code', ''),
            'iv': float(data.get('implied_volatility', '0') or '0'),
            'oi': data.get('open_interest', 0),
            'bid': float(data.get('nbbo_bid', '0') or '0'),
            'ask': float(data.get('nbbo_ask', '0') or '0'),
            'theo': float(data.get('theo', '0') or '0'),
            'delta': float(data.get('delta', '0') or '0'),
            'gamma': float(data.get('gamma', '0') or '0'),
            'vega': float(data.get('vega', '0') or '0'),
            'theta': float(data.get('theta', '0') or '0'),
            'rho': float(data.get('rho', '0') or '0'),
            'long_vol': data.get('ask_vol', 0),
            'short_vol': data.get('bid_vol', 0),
            'other_vol': data.get('no_side_vol', 0),
            'mid_vol': data.get('mid_vol', 0),
            'leg_vol': data.get('multi_vol', 0),
            'stock_vol': data.get('stock_multi_vol', 0),
            'vol': data.get('volume', 0),
            'tags': tags
        }
        return result
    except Exception as e:
        logger.error("Error mapping fields: %s", e)
        return None




class UnusualWhalesSource(CustomSource):
    """External Source for the UnusualWhales Options Websocket API"""
    def __init__(self, name: str):  # noqa E501
        super().__init__(name=name)
        print(f"UnusualWhalesToken: {os.environ['UNUSUALWHALES_TOKEN']}")

        self.uri = f"wss://api.unusualwhales.com/socket?token={os.environ['UNUSUALWHALES_TOKEN']}"  # noqa E501
        self.name = name
        self._producer_topic = Topic(name="option_trades", key_serializer='string', value_serializer='json')

    def run(self):
        logger.info("Processing WebSocket messages...")
        while self.running:
            logger.info("Connecting to WebSocket...")
            try:
                with connect(
                    self.uri,
                    logger=logger,
                ) as ws:
                    subscribe_message = json.dumps({
                        "channel": "option_trades",
                        "msg_type": "join"
                    })
                    ws.send(subscribe_message)
                    logger.info("""
                        Successfully subscribed to the UnusualWhales API https://api.unusualwhales.com.
                    """)
                    for message in ws:
                        try:
                            data = json.loads(message)
                            for item in data[1:]:  # Skip the first position as it's never a valid option trade record
                                if item.get('price'):  # noqa E501
                                    record = map_fields(item)
                                    if record:
                                        msg_headers = {
                                            "data_provider": "UnusualWhales",
                                            "integration_id": record.get('id')
                                        }
                                        msg = self.serialize(key=record.get('osym'), value=record, headers=msg_headers, timestamp_ms=record.get('ts'))
                                        self.produce(
                                            key=record.get('osym'),
                                            value=json.dumps(record),
                                            poll_timeout=2.0,
                                            buffer_error_max_tries=3,
                                            timestamp=msg.timestamp_ms,
                                            headers=msg.headers
                                        )
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON message: {e}")
                        except Exception as e:
                            print(f"Error processing message: {e}")
            except websockets.exceptions.ConnectionClosedError as e:
                print(f"Connection closed with error: {e}. Reconnecting...")
                time.sleep(5)  # Wait before reconnecting
                continue
            except Exception as e:
                print(f"Unexpected error: {e}. Reconnecting...")
                time.sleep(5)  # Wait before reconnecting

