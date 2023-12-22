import ccxt

exchange = ccxt.okex5()

print(exchange.public_get_market_index_tickers({'quoteCcy': "USDT"}))