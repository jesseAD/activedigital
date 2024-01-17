import ccxt


params = {
    'apiKey': "bd7dbeeb-d6d1-42e6-9c88-8d7f7b350154",
    'secret': "3B92EDEDA9AF108941949A938CF98539",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': True,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    'password': "Jesse123!"
}

exchange = ccxt.okex5(params)
exchange.private_get_account_max_loan(params={
    'mgnMode': "cross",
    'instId': "BTC-USDT"
    })
# print(exchange.fetch_balance())