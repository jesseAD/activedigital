import json
import logging
import pymongo
import boto3
import concurrent.futures

from src.config import read_config_file
from src.handlers.instantiator import leverages_wrapper
# from src.handlers.instantiator import get_data_collectors

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

  executor = concurrent.futures.ThreadPoolExecutor(config['dask']['threadsPerPool'])
  threads = []

  for client in config['clients']:
    for exchange in config['clients'][client]['subaccounts']:
      for account in config['clients'][client]['subaccounts'][exchange]:
        if account != "base_ccy":
          threads.append(leverages_wrapper(executor, client, exchange, account, db))

  for thread in concurrent.futures.as_completed(threads):
    print(thread.result())
    thread.cancel()

  db.client.close()
  print("Finished Leverage")

  

  # TODO implement
  return {
    'statusCode': 200,
    'body': json.dumps("Finished Leverage")
  }
