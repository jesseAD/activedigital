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
db = pymongo.MongoClient(mongo_uri)
collection = db['active_digital']['balances']

for client in config['clients']:
  for exchange in config['clients'][client]['subaccounts']:
    for account in config['clients'][client]['subaccounts'][exchange]:
      balance = list(collection.find({
        'client': client,
        'venue': exchange,
        'account': account,
        'runid': 70651
      }))

      if len(balance) > 0:
        vip_level = balance[0]['vip_level']

        collection.update_many(
          {'runid': {'$lt': 70651}, 'client': client, 'venue': exchange, 'account': account},
          {'$set': {'vip_level': vip_level}}
        )
      # FundingContributions(db, "temp_collection").create(
      #   client=client,
      #   exchange=exchange,
      #   account=account
      # )