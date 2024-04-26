import os, sys, json
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
    'apiKey': "",
    'secret': "",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': False,
    'options': {
        'adjustForTimeDifference':True,
        'warnOnFetchOpenOrdersWithoutSymbol': False
    },
    'headers': {},
    # 'password': "!"
}

exchange = ccxt.huobi(params)
res = exchange.fetch_open_orders(params={})
# res = exchange.fetch_positions(params={'marginMode': "cross", 'subType': "linear"})
print(res)
# res = [
#   {
#     'id': item['id'],
#     'symbol': item['symbol'],
#     'info': item['info'],
#   }
#   for item in res
# ]
# with open('huobi.txt', 'w') as f:
#   f.write(json.dumps(res))

# print([item['info']['sn'] for item in res])