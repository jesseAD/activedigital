import ccxt
from dotenv import load_dotenv
from src.config import read_config_file

config = read_config_file()
load_dotenv()

# Create an echange class instance
class Exchange:
  def __init__(self, exchange = None, account = None, key = None, secret = None, passphrase = None):
    self.exchange = exchange
    self.sub_account = account
    self.key = key
    self.secret = secret
    self.passphrase = passphrase

  def exch(self):
    
    params = {
      'apiKey': self.key,
      'secret': self.secret,
      'enableRateLimit': True,
      'rateLimit': config['ccxt'][self.exchange]['rateLimit'],
      'timeout': config['ccxt'][self.exchange]['timeout'],
      'requests_trust_env':True,
      'verbose': False,
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
      params['password'] = self.passphrase
      # exchange = ccxt.okex5(params)
      exchange = ccxt.okx(params)
    elif self.exchange == 'coinbase':
      exchange = ccxt.coinbasepro()
    elif self.exchange == 'huobi':
      exchange = ccxt.huobi(params)
    
    return exchange

    