import os, sys, json, csv
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
from src.lib.apr_pairs import filter_insts, make_pairs
from src.handlers.aprs import Aprs
# from src.lib.log import Log

load_dotenv()
# logger = Log()

mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
# db = pymongo.MongoClient(mongo_uri)['active_digital']
# db = pymongo.MongoClient(None)['active_digital']
db = pymongo.MongoClient(None)

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

# exchange = ccxt.deribit(params)
# response = exchange.fetch_tickers(params={'currency': "USDC"})
# print(response['BTC/USDC:USDC'])

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


# insts = list(db['instruments'].find({}))
# insts = {item['venue']: item['instrument_value'] for item in insts}

# insts = filter_insts(insts)
# pairs = make_pairs(insts)

# pairs = [
#   [
#     item['leg1_exchange'], 
#     item['leg1'], 
#     item['leg1_type'], 
#     item['leg1_expiry'], 
#     item['leg2_exchange'], 
#     item['leg2'], 
#     item['leg2_type'], 
#     item['leg2_expiry'], 
#   ] for item in pairs
# ]

# with open('insts.json', 'w') as file:
#   json.dump(insts, file, indent=2)

# with open('data.csv', 'w', newline='') as csv_file:
#   writer = csv.writer(csv_file)

#   writer.writerows(pairs)

# aprs = Aprs(db, "aprs")
# aprs.create()

days = round((1726214400000 - 1726084800000) / 86400000)
print(days)