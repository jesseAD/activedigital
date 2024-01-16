import ccxt


params = {
    'apiKey': "28d103d5-04a0-4102-a1ab-5ac12459b089",
    'secret': "5FA27159C3FD4A3BEE2ECD5E9FA87A79",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': False,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    'password': "Jesse123!"
}

exchange = ccxt.okex5(params)

print(exchange.private_get_account_interest_limits(params={'type': "1", 'ccy': "USDT"}))