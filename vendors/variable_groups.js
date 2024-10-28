model_variable_groups = {
   'stock_trade_vars': ['stock_sell_trade_ct', 'stock_sell_vol', 'stock_buy_ct', 'stock_buy_vol', 'stock_mid_ct', 'stock_mid_vol'],
   'option_trade_vars': ['call_buy_vol', 'call_buy_prem', 'call_buy_count', 'call_sell_vol', 'call_sell_prem', 'call_sell_count', 'call_mid_vol', 'call_mid_prem', 'call_mid_ct', 'put_buy_vol', 'put_buy_prem', 'put_buy_trades', 'put_sell_vol', 'put_sell_prem', 'put_sell_trades', 'put_mid_vol', 'put_mid_prem', 'put_mid_ct'],
   'stock_price_vars': ['open_price', 'high_price', 'low_price', 'close_price', 'vwap', 'price_range'],
   'book_vars': ['nbbo_bid_changes', 'nbbo_ask_changes'],
   'darkpool_vars': ['darkpool_vol', 'darkpool_ct', 'darkpool_vwap'],
   'news_vars': ['news_ct', 'news_sentiment_score', 'news_sentiment_strength'],
}