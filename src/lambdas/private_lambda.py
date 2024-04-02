import json
import logging
import pymongo
import boto3
import concurrent.futures

from src.config import read_config_file
from src.handlers.instantiator import balances_wrapper, positions_wrapper, fills_wrapper, transactions_wrapper, open_orders_wrapper
from src.handlers.instantiator import get_data_collectors

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

  private_data_collectors = [
    balances_wrapper, positions_wrapper, open_orders_wrapper,
    fills_wrapper, transactions_wrapper
  ]

  accounts = get_data_collectors(event['client'], secrets)

  executor = concurrent.futures.ThreadPoolExecutor(config['dask']['threadsPerPool'])
  threads = []

  for data_collector in private_data_collectors:
    for account in accounts:
      threads.append(data_collector(executor, account.client, account, db))

  for thread in concurrent.futures.as_completed(threads):
    print(thread.result())
    thread.cancel()

  db.client.close()
  print("Finished Private")

  

  # TODO implement
  return {
    'statusCode': 200,
    'body': json.dumps("Finished Private")
  }
