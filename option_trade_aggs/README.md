# option_trade_aggs

produces: `option-trade-aggs`
consumes: `option-trades`

groups by: 'option symbol (osym)'
window: 1 minute
window_type: tumbling

meta: {
    "osym": "option symbol",
    "window_start": "window start timestamp"

}

start: int
end: int

value: {
    "option_symbol": "AAPL240118C0010000",
    "date": "2024-01-01",
    "underlying_symbol": "APPL",
    "strike_price": 100,
    "expiration_date": "2024-01-18",
    "option_type": "call",
    "stats": {
        "call_ask_volume": 100,
        "call_bid_volume": 100,
        "call_mid_volume": 100,
        "call_ask_premium": 100,
        "call_bid_premium": 100,
        "call_mid_premium": 100,
        "bullish_trade_
        "bullish_volume": 100,
        "bearish_volume": 100,
        "neutral_volume": 100,
    }
}

<!-- volume is the sum of all volumes for each side -->
call_ask_volume: int
call_bid_volume: int
call_mid_volume: int
put_ask_volume: int
put_bid_volume: int
put_mid_volume: int

<!-- premium is the sum of all premiums for each side -->
call_ask_premium: float
call_bid_premium: float
call_mid_premium: float
put_ask_premium: float
put_bid_premium: float
put_mid_premium: float

<!-- number of trades is the sum of all trades for each side -->
num_of_bullish_trades: int
num_of_bearish_trades: int
num_of_neutral_trades: int

<!-- volume is the sum of all volumes for each side -->
bullish_volume: int
bearish_volume: int
neutral_volume: int

<!-- premium is the sum of all premiums for each side -->
bullish_premium: float
bearish_premium: float
neutral_premium: float
