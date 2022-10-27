import ccxt


# Create an echange class instance
class Exchange:
    def __init__(self, account, key, secret):
        self.sub_account = account
        self.key = key
        self.secret = secret

    def ftx(self):
        
        params = {
            'apiKey': self.key,
            'secret': self.secret,
            'enableRateLimit': True,
        }

        if self.sub_account:
            params['headers'] = {
                'FTX-SUBACCOUNT': self.sub_account,
            }
        
        exchange = ccxt.ftx(params)

        return exchange

        