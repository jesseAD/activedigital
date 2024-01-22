import os, sys
import ccxt
import pymongo

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.positions import Positions
from src.lib.log import Log


# params = {
#     'apiKey': "426559af-fae9-4f16-9e55-c36179c5fded",
#     'secret': "1657FB9643F7BD8E6A55C758C6EBF1BD",
#     'enableRateLimit': True,
#     'requests_trust_env':True,
#     'verbose': True,
#     'options': {
#         'adjustForTimeDifference':True,
#     },
#     'headers': {},
#     'password': "Active2023!"
# }

# exchange = ccxt.okex5(params)
# exchange.private_get_account_interest_limits(params={
#     'type': 1,
#     'ccy': ["USDT", "BTC"]
#     })

logger = Log()

mongo_uri = 'mongodb+srv://activedigital:'+'pwd'+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digital']

params = {
    'apiKey': "cbde5d6b-8116-4cf9-90d3-b9fa213ce4c1",
    'secret': "B2632C6F1E933253150C3791998CFEAF",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': False,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    'password': "ActiveTrade2023!"
}

exchange = ccxt.okex5(params)

Positions(db, 'positions').create(
    client="edison",
    exch=exchange,
    exchange="okx",
    sub_account="submn1", 
    logger=logger
)

# print(exchange.fetch_balance())