import os, sys, json
import ccxt
import pymongo
import math
# import pdb
import numpy as np
from datetime import datetime, timezone, timedelta
import time
from dateutil import tz, relativedelta

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.daily_returns import DailyReturns
# from src.lib.log import Log

# logger = Log()

mongo_uri = 'mongodb+srv://activedigital:'+''+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri)

DailyReturns(db, "daily_returns").create(
  client="nifty",
  exchange="binance",
  account="subbasis1",
  balance_finished={"nifty_binance_subbasis1": True}
)

# params = {
#     'apiKey': "",
#     'secret': "",
#     'enableRateLimit': True,
#     'requests_trust_env':True,
#     'verbose': True,
#     'options': {
#         'adjustForTimeDifference':True,
#         'warnOnFetchOpenOrdersWithoutSymbol': False
#     },
#     'headers': {},
#     # 'password': "!"
# }

# exchange = ccxt.okx(params)
# res = exchange.private_get_account_interest_rate(params={"ccy": "LRC"})
# res = exchange.fetch_borrow_rate_history(code="LRC", limit=92)
# print(res)

# res = exchange.fetch_positions(params={'type': "swap", 'subType': "inverse"})
# contracts = [item['contract_code'] for item in res]

# transactions = []
# for contract in contracts:
#     transactions += exchange.contract_private_post_linear_swap_api_v3_swap_financial_record(params={**params, 'mar_acct': contract})['data']

# accounts = [int(item['id']) for item in res]

# transactions = []
# for account in accounts:
#     transactions += exchange.spot_private_get_v1_account_history(params={'account-id': account})['data']
# res = exchange.fetch_positions(params={'marginMode': "cross", 'subType': "linear"})
# print(res)
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