import ccxt


params = {
    'apiKey': "426559af-fae9-4f16-9e55-c36179c5fded",
    'secret': "1657FB9643F7BD8E6A55C758C6EBF1BD",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': False,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    'password': "Active2023!"
}

exchange = ccxt.okex5(params)

print(exchange.private_get_account_interest_limits(params={'type': "1", 'ccy': "USDT"}))