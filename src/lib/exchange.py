import ccxt
import os
from dotenv import load_dotenv
load_dotenv()

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
            'requests_trust_env':True,
            'options': {
                'adjustForTimeDifference':True,
            }
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
            passphrase = os.getenv('OKX_'+self.sub_account.upper()+'_PASSPHRASE')
            params['password'] = passphrase
            exchange = ccxt.okex5(params)
        
        return exchange

        