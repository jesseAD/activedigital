import json
import logging
import pymongo
import boto3
import concurrent.futures

from src.config import read_config_file
from src.lib.exchange import Exchange
from src.handlers.instantiator import instruments_wrapper
from src.handlers.instantiator import tickers_wrapper
from src.handlers.instantiator import index_prices_wrapper
from src.handlers.instantiator import borrow_rates_wrapper
from src.handlers.instantiator import funding_rates_wrapper
from src.handlers.instantiator import mark_prices_wrapper
from src.handlers.instantiator import bids_asks_wrapper
from src.handlers.instantiator import roll_costs_wrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
  config = read_config_file()

  secrets = json.loads(
    boto3.session.Session().client(
      service_name='secretsmanager', 
      region_name="eu-central-1"
    ).get_secret_value(SecretId="activedigital_secrets")['SecretString']
  )
  mongo_uri = 'mongodb+srv://activedigital:'+ secrets['CLOUD_MONGO_PASSWORD'] +'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
  db = pymongo.MongoClient(mongo_uri, maxPoolsize=config['mongodb']['max_pool'])['active_digita']

  public_data_collectors = [
    instruments_wrapper, tickers_wrapper, roll_costs_wrapper,
    mark_prices_wrapper, index_prices_wrapper, bids_asks_wrapper,
    funding_rates_wrapper, borrow_rates_wrapper
  ]

  latest_positions = list(db['positions'].aggregate([
    {"$group": {
      "_id": {"client": "$client", "venue": "$venue", "account": "$account"},
      "position_value": {"$last": "$position_value"}
    }},
    {"$unwind": "$position_value"},
    {"$project": {
      "symbol": "$position_value.base", "_id": 0
    }},
    {"$group": {
      "_id": {"symbol": "$symbol"},
      "symbol": {"$last": "$symbol"}
    }},
    {"$project": {"_id": 0}}
  ]))
  latest_balances = list(db['balances'].aggregate([
    {"$group": {
      "_id": {"client": "$client", "venue": "$venue", "account": "$account"},
      "balance_value": {"$last": "$balance_value"}
    }},
    {"$project": {
      "_id": 0
    }},
  ]))

  symbols = [item['symbol'] for item in latest_positions if item['symbol'] != None and item['symbol'] not in config['symbols']]
  symbols += config['symbols']

  for balance in latest_balances:
    for _key in balance['balance_value']:
      if _key != "USD" and _key != "base" and _key not in symbols:
        symbols.append(_key)

  exch = Exchange(event['exchange']).exch()

  executor = concurrent.futures.ThreadPoolExecutor(config['dask']['threadsPerPool'])
  threads = []

  for collector in public_data_collectors:
    threads += collector(executor, exch, event['exchange'], symbols, db)

  for thread in concurrent.futures.as_completed(threads):
    print(thread.result())
    thread.cancel()

  db.client.close()
  print("Finished Public")

  

  # TODO implement
  return {
    'statusCode': 200,
    'body': json.dumps("Finished Public")
  }
