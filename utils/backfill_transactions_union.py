import os, sys, json
import pymongo
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import concurrent.futures

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.funding_contributions import FundingContributions
from src.config import read_config_file

config = read_config_file()
load_dotenv()

mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri)['active_digital']

accounts = list(db['transactions'].aggregate([
  {'$group': {
    '_id': {'client': "$client", 'venue': "$venue", 'account': "$account"},
    'client': {'$last': '$client'},
    'venue': {'$last': '$venue'},
    'account': {'$last': '$account'}
  }},
  {'$project': {'_id': 0}},
  {'$sort': {'client': 1, 'venue': 1, 'account': 1}}
]))

# accounts = accounts[8:]

print(accounts)

def backfill_account(account):
# for account in accounts:
  print(account)

  runids_done = list(db['transactions_union'].aggregate([
    {'$match': {'client': account['client'], 'venue': account['venue'], 'account': account['account'], 'runid': {'$lt': 71647}}},
    {'$sort': {'runid': 1}},
    {'$group': {
      '_id': None,
      'runid': {'$last': "$runid"}
    }}
  ]))

  if len(runids_done) == 0:
    return
  
  runids = list(db['transactions'].aggregate([
    {'$match': {'client': account['client'], 'venue': account['venue'], 'account': account['account'], 'runid': {'$lt': 71647, '$gt': runids_done[0]['runid']}}},
    {'$group': {'_id': "$runid"}},
    {'$sort': {'_id': 1}}
  ]))

  runids = [item['_id'] for item in runids]

  for runid in runids:
    print(str(account) + " ---> " + str(runid))

    transactions = list(db['transactions'].find({
      'client': account['client'],
      'venue': account['venue'],
      'account': account['account'],
      'runid': runid
    }))

    transactions_union = []

    for item in transactions:
      income_type = ""

      if account['venue'] == "binance":
        if item['transaction_value']['incomeType'] == "TRANSFER" and item['transaction_value']['income'] > 0:
          income_type = "COIN_SWAP_DEPOSIT"
        elif item['transaction_value']['incomeType'] == "TRANSFER" and item['transaction_value']['income'] < 0:
          income_type = "COIN_SWAP_WITHDRAW"
        if item['transaction_value']['incomeType'] == "INTERNAL_TRANSFER" and item['transaction_value']['income'] > 0:
          income_type = "COIN_SWAP_DEPOSIT"
        elif item['transaction_value']['incomeType'] == "INTERNAL_TRANSFER" and item['transaction_value']['income'] < 0:
          income_type = "COIN_SWAP_WITHDRAW"
        elif item['transaction_value']['incomeType'] == "COMMISSION":
          income_type = "COMMISSION"
        elif item['transaction_value']['incomeType'] == "COMMISSION_REBATE":
          income_type = "COMMISSION"
        elif item['transaction_value']['incomeType'] == "API_REBATE":
          income_type = "COMMISSION"
        elif item['transaction_value']['incomeType'] == "FUNDING_FEE":
          income_type = "FUNDING_FEE"
        elif item['transaction_value']['incomeType'] == "ON_BORROW":
          income_type = "BORROW"
        elif item['transaction_value']['incomeType'] == "PERIODIC":
          income_type = "BORROW"

        transactions_union.append({
          **item,
          'transaction_value': {
            'info': item['transaction_value']['info'],
            'symbol': item['transaction_value']['symbol'] if "symbol" in item['transaction_value'] else "",
            'asset': item['transaction_value']['asset'],
            'income': item['transaction_value']['income'],
            'income_base': item['transaction_value']['income_base'] if "income_base" in item['transaction_value'] else 0,
            'income_origin': item['transaction_value']['income_origin'] if "income_origin" in item['transaction_value'] else 0,
            'timestamp': item['transaction_value']['timestamp'],
          },
          'incomeType': income_type
        })
    
      elif account['venue'] == "okx":
        if item['transaction_value']['type'] == "2":
          income_type = "COMMISSION"
          income = item['transaction_value']['fee']
          income_base = item['transaction_value']['fee_base'] if "fee_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['fee_origin'] if "fee_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['type'] == "8":
          income_type = "FUNDING_FEE"
          income = item['transaction_value']['pnl']
          income_base = item['transaction_value']['pnl_base'] if "pnl_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['pnl_origin'] if "pnl_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['type'] == "15":
          income_type = "BORROW"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "9":
          income_type = "BORROW"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "11":
          income_type = "COIN_SWAP_DEPOSIT"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "12":
          income_type = "COIN_SWAP_WITHDRAW"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "173":
          income_type = "FUNDING_FEE"
          income = item['transaction_value']['pnl']
          income_base = item['transaction_value']['pnl_base'] if "pnl_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['pnl_origin'] if "pnl_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "174":
          income_type = "FUNDING_FEE"
          income = item['transaction_value']['pnl']
          income_base = item['transaction_value']['pnl_base'] if "pnl_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['pnl_origin'] if "pnl_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "210":
          income_type = "BORROW"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "212":
          income_type = "BORROW"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0
        elif item['transaction_value']['subType'] == "17":
          income_type = "BORROW"
          income = item['transaction_value']['sz']
          income_base = item['transaction_value']['sz_base'] if "sz_base" in item['transaction_value'] else 0
          income_origin = item['transaction_value']['sz_origin'] if "sz_origin" in item['transaction_value'] else 0

        transactions_union.append({
          **item,
          'transaction_value': {
            'info': item['transaction_value']['info'],
            'symbol': item['transaction_value']['instId'] if "instId" in item['transaction_value'] else "",
            'asset': item['transaction_value']['ccy'],
            'income': income,
            'income_base': income_base,
            'income_origin': income_origin,
            'timestamp': item['transaction_value']['timestamp'],
          },
          'incomeType': income_type
        })

      elif account['venue'] == "bybit":
        income = item['transaction_value']['fee']
        income_base = item['transaction_value']['fee_base'] if "fee_base" in item['transaction_value'] else 0
        income_origin = item['transaction_value']['fee_origin'] if "fee_origin" in item['transaction_value'] else 0

        if item['trade_type'] == "borrow":
          income_type = "BORROW"
        else:
          if item['transaction_value']['type'] == "TRADE":
            income_type = "COMMISSION"
          elif item['transaction_value']['type'] == "SETTLEMENT":
            income_type = "FUNDING_FEE"
            income = item['transaction_value']['funding']
            income_base = item['transaction_value']['funding_base'] if "funding_base" in item['transaction_value'] else 0
            income_origin = item['transaction_value']['funding_origin'] if "funding_origin" in item['transaction_value'] else 0
          elif item['transaction_value']['type'] == "TRANSFER_IN":
            income_type = "COIN_SWAP_DEPOSIT"
            income = item['transaction_value']['cashFlow']
            income_base = item['transaction_value']['cashFlow_base'] if "cashFlow_base" in item['transaction_value'] else 0
            income_origin = item['transaction_value']['cashFlow_origin'] if "cashFlow_origin" in item['transaction_value'] else 0
          elif item['transaction_value']['type'] == "TRANSFER_OUT":
            income_type = "COIN_SWAP_WITHDRAW"
            income = item['transaction_value']['cashFlow']
            income_base = item['transaction_value']['cashFlow_base'] if "cashFlow_base" in item['transaction_value'] else 0
            income_origin = item['transaction_value']['cashFlow_origin'] if "cashFlow_origin" in item['transaction_value'] else 0

        transactions_union.append({
          **item,
          'transaction_value': {
            'info': item['transaction_value']['info'],
            'symbol': item['transaction_value']['symbol'] if "symbol" in item['transaction_value'] else "",
            'asset': item['transaction_value']['currency'],
            'income': income,
            'income_base': income_base,
            'income_origin': income_origin,
            'timestamp': item['transaction_value']['timestamp'],
          },
          'incomeType': income_type
        })

      elif account['venue'] == "huobi":
        if item['transaction_value']['type'] == "30" or item['transaction_value']['type'] == "31":
          income_type = "FUNDING_FEE"
        elif (
          item['transaction_value']['type'] == "5" or 
          item['transaction_value']['type'] == "6" or 
          item['transaction_value']['type'] == "7" or 
          item['transaction_value']['type'] == "8" or
          item['trade_type'] == "spot"
        ):
          income_type = "COMMISSION"
        elif (
          item['transaction_value']['type'] == "14" or 
          item['transaction_value']['type'] == "34" or 
          item['transaction_value']['type'] == "37" or 
          item['transaction_value']['type'] == "38" 
        ):
          income_type = "COIN_SWAP_DEPOSIT"
        elif (
          item['transaction_value']['type'] == "15" or 
          item['transaction_value']['type'] == "36" or 
          item['transaction_value']['type'] == "39" 
        ):
          income_type = "COIN_SWAP_WITHDRAW"

        transactions_union.append({
          **item,
          'transaction_value': {
            'info': item['transaction_value']['info'],
            'symbol': item['transaction_value']['contract_code'] if "contract_code" in item['transaction_value'] else "",
            'asset': item['transaction_value']['currency'],
            'income': item['transaction_value']['amount'],
            'income_base': item['transaction_value']['amount_base'] if "amount_base" in item['transaction_value'] else 0,
            'income_origin': item['transaction_value']['amount_origin'] if "amount_origin" in item['transaction_value'] else 0,
            'timestamp': item['transaction_value']['timestamp'],
          },
          'incomeType': income_type
        })

      elif account['venue'] == "deribit":
        if (
          item['transaction_value']['type'] == "maker" or 
          item['transaction_value']['type'] == "taker" or 
          item['transaction_value']['type'] == "open" or 
          item['transaction_value']['type'] == "close" or
          item['transaction_value']['type'] == "liquidation" or
          item['transaction_value']['type'] == "buy" or
          item['transaction_value']['type'] == "sell" or
          item['transaction_value']['type'] == "delivery" or
          item['transaction_value']['type'] == "option" or
          item['transaction_value']['type'] == "future" or
          item['transaction_value']['type'] == "correction" or
          item['transaction_value']['type'] == "block_trade"
        ):
          income_type = "COMMISSION"
        elif item['transaction_value']['type'] == "deposit":
          income_type = "COIN_SWAP_DEPOSIT"
        elif item['transaction_value']['type'] == "withdrawal":
          income_type = "COIN_SWAP_WITHDRAW"

        transactions_union.append({
          **item,
          'transaction_value': {
            'info': item['transaction_value']['info'],
            'symbol': item['transaction_value']['instrument_name'] if "instrument_name" in item['transaction_value'] else "",
            'asset': item['transaction_value']['currency'],
            'income': item['transaction_value']['change'],
            'income_base': item['transaction_value']['change_base'] if "change_base" in item['transaction_value'] else 0,
            'income_origin': item['transaction_value']['change_origin'] if "change_origin" in item['transaction_value'] else 0,
            'timestamp': item['transaction_value']['timestamp'],
          },
          'incomeType': income_type
        })

    db['transactions_union'].insert_many(transactions_union)

  print("Finished " + str(account))


for account in accounts:
  backfill_account(account)

# executor = concurrent.futures.ThreadPoolExecutor(50)
# threads = []

# for account in accounts:
#   executor.submit(backfill_account, account)

# for thread in concurrent.futures.as_completed(threads):
#   thread.cancel()