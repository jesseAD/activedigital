import ccxt

exchange = ccxt.bybit()

print(exchange.fetch_tickers()['BONK/USDT:USDT'])