import ccxt


# Create an echange class instance
class Exchange:
    def __init__(self, exchange, account, key, secret):
        self.exchange = exchange
        self.sub_account = account
        self.key = key
        self.secret = secret

    def exch(self):
        
        params = {
            'apiKey': self.key,
            'secret': self.secret,
            'enableRateLimit': True,
        }

        if self.sub_account:
            params['headers'] = {
                self.exchange.upper()+'-SUBACCOUNT': self.sub_account,
            }
        if self.exchange == 'binance':
            exchange = ccxt.binance(params)
        elif self.exchange == 'bybit':
            exchange = ccxt.bybit(params)
        elif self.exchange == 'okx':
            exchange = ccxt.okx(params)
        
        return exchange

        