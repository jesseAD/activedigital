import os, sys, json
import ccxt
import pymongo
import math
# import pdb
import time
from dateutil import tz, relativedelta
import os, sys, json
import pymongo
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.daily_returns import DailyReturns
from src.handlers.funding_contributions import FundingContributions
# from src.lib.log import Log

load_dotenv()
# logger = Log()

mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri)['active_digital']

# DailyReturns(db, "daily_returns").create(
#   client="shannon",
#   exchange="okx",
#   account="subls1",
# )

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
  # 'password': "dcj5*kTT7%"
}

exchange = ccxt.binance(params)
response = exchange.sapi_get_sub_account_transfer_subuserhistory(params={})
print(response)
for i in range(8):
  start = int(datetime(2024, i+1, 1).timestamp() * 1000)
  end = int(datetime(2024, i+2, 1).timestamp() * 1000)
  print(start)

  response = exchange.sapi_get_sub_account_transfer_subuserhistory(params={
    'startTime': start,
    'endTime': end,
    'type': 2,
    'limit': 1000,
  })
  print(response)

  transactions = []
  transactions_union = []
  for item in response:
    date = datetime.fromtimestamp(int(item['time']) / 1000)

    run = list(db['runs'].find({'start_time': {'$gte': date}}).sort('runid', 1).limit(1))[0]

    transactions.append({
      'transaction_value': {
        'info': item,
        'asset': item['asset'],
        'incomeType': "TRANSFER",
        'income': -float(item['qty']),
        'income_base': -float(item['qty']),
        'income_origin': -float(item['qty']),
        'timestamp': int(item['time']) - 600000
      },
      'client': "blackburn",
      'venue': "binance",
      'account': "submn1",
      'runid': run['runid'],
      'timestamp': run['end_time'],
      'convert_ccy': "USDT",
      'trade_type': "um"
    })

    transactions_union.append({
      'transaction_value': {
        'info': item,
        'asset': item['asset'],
        'symbol': "",
        'income': -float(item['qty']),
        'income_base': -float(item['qty']),
        'income_origin': -float(item['qty']),
        'timestamp': int(item['time']) - 600000
      },
      'client': "blackburn",
      'venue': "binance",
      'account': "submn1",
      'runid': run['runid'],
      'timestamp': run['end_time'],
      'convert_ccy': "USDT",
      'trade_type': "um",
      'incomeType': "COIN_SWAP_WITHDRAW"
    })

  print(transactions)
  print(transactions_union)
  if len(transactions) > 0:
    db['transactions'].insert_many(transactions)
    db['transactions_union'].insert_many(transactions_union)

# print(response)
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