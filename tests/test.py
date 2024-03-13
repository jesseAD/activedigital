import os, sys
import ccxt
# import pymongo
# import pdb
from datetime import datetime, timezone
import time

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

# from src.handlers.positions import Positions
# from src.lib.log import Log

# logger = Log()

# mongo_uri = 'mongodb+srv://activedigital:'+'pwd'+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
# db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digital']

params = {
    'apiKey': "api",
    'secret': "secret",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': False,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    # 'password': "pwd"
}

# exchange = ccxt.binance(params)
# exchange.private_get_mytrades()
exchage = ccxt.bybit(params)


exchage.fetch_my_trades(symbol="FILUSDT")
             

# print(exchange.papi_get_balance())
# print(exchange.fapiprivatev2_get_account())

# exchange.sapi_get_margin_tradecoeff()
# exchange.fapiprivatev2_get_balance()


# print(exchange.fetch_account_positions(params={"type": "future"}))

# exchange = ccxt.okx(params)

# transactions = []

# while(True):
#     end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
#     res = exchange.private_get_account_bills_archive(params={"end": end_time})["data"]

#     if len(res) == 0:
#         break

#     res.sort(key = lambda x: x['ts'])
#     transactions = res + transactions
#     print(res)
#     time.sleep(1)

# print(len(transactions))
    