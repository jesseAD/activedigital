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
from src.handlers.funding_contributions import FundingContributions
# from src.lib.log import Log

# logger = Log()

# mongo_uri = 'mongodb+srv://activedigital:'+''+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
# db = pymongo.MongoClient(mongo_uri)

DailyReturns(db, "daily_returns").create(
  client="shannon",
  exchange="okx",
  account="subls1",
)

params = {
    'apiKey': "aa",
    'secret': "aa",
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

# exchange = ccxt.deribit(params)
# res = exchange.private_get_get_account_summary(params={'currency': "USDC"})
# print(res)
# res = exchange.public_get_v5_market_kline(params={'category': "spot", 'symbol': "BTCUSDT", 'interval': "1", 'limit': 1, 'start': 1708500592000})
# print(res)
# exchange.papi_get_balance()

# for runid in range(52255, 67355):
#   balance = list(db['active_digital']['balances'].find({
#     'client': "nifty",
#     'venue': "bybit",
#     'account': "subbasis1",
#     'runid': runid
#   }))
#   print(balance)

#   if len(balance) > 0:
#     price = float(exchange.public_get_v5_market_kline(
#       params={'category': "spot", 'symbol': "BTCUSDT", 'interval': "1", 'limit': 1, 'start': int(balance[0]['timestamp'].timestamp() * 1000)}
#     )['result']['list'][0][4])
#     print(price)

#     db['active_digital']['balances'].update_one(
#       {
#         'client': "nifty",
#         'venue': "bybit",
#         'account': "subbasis1",
#         'runid': runid
#       },
#       {'$set': {
#         'collateral': 7.5 * price
#       }}
#     )
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