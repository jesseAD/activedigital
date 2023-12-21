import ccxt

exchange = ccxt.okex5()

print(exchange.public_get_public_mark_price({'instType': "SWAP"}))