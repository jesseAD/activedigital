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
    'verbose': True,
    'options': {
        'adjustForTimeDifference':True,
        'warnOnFetchOpenOrdersWithoutSymbol': False
    },
    'headers': {},
    # 'password': "!"
}

exchange = ccxt.huobi(params)

res = exchange.contract_private_post_swap_api_v1_swap_account_position_info(params={'contract_code': "DOGE-USD"})['data']
# contracts = [item['contract_code'] for item in res]

# transactions = []
# for contract in contracts:
#     transactions += exchange.contract_private_post_linear_swap_api_v3_swap_financial_record(params={**params, 'mar_acct': contract})['data']

# accounts = [int(item['id']) for item in res]

# transactions = []
# for account in accounts:
#     transactions += exchange.spot_private_get_v1_account_history(params={'account-id': account})['data']
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