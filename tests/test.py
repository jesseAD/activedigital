import os, sys
import ccxt
import pymongo

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.positions import Positions
from src.lib.log import Log

logger = Log()

mongo_uri = 'mongodb+srv://activedigital:'+'pwd'+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digital']

params = {
    'apiKey': "api",
    'secret': "secret",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': True,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    'password': "pwd"
}

exchange = ccxt.okex5(params)

# exchange.sapi_get_margin_tradecoeff()
# exchange.fapiprivatev2_get_balance()

Positions(db, 'positions').create(
    client="faraday",
    exch=exchange,
    exchange="okx",
    sub_account="subls", 
    logger=logger
)

# print(exchange.private_get_account_interest_limits(params={'type': 1, 'ccy': "BTC"}))