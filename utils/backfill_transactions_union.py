import os, sys, json
import pymongo
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

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

runids = list(db['transactions'].aggregate([
  {'$match': {'client': "blackburn", 'venue': "bybit", 'runid': {'$lt': 71647}}},
  {'$group': {'_id': "$runid"}},
  {'$sort': {'_id': 1}}
]))

runids = [item['_id'] for item in runids]

for runid in runids:
  print(runid)

  transactions = list(db['transactions'].find({
    'client': "blackburn",
    'venue': "bybit",
    'runid': runid
  }))

  transactions_union = []

  for item in transactions:
    income_type = ""

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

  db['transactions_union'].insert_many(transactions_union)
