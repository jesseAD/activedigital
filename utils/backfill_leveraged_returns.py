import os, sys, json
import pymongo
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.daily_returns import DailyReturns
from src.config import read_config_file

config = read_config_file()
load_dotenv()

mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri)['active_digital']

for client in config['clients']:
  for exchange in config['clients'][client]['subaccounts']:
    for account in config['clients'][client]['subaccounts'][exchange]:
      if config['clients'][client]['subaccounts'][exchange][account]['daily_returns'] == True:
        print(client+exchange+account)
        daily_returns = list(db['daily_returns'].find({
          'client': client,
          'venue': exchange,
          'account': account
        }))

        for item in daily_returns:
          leverages = list(db['leverages'].find({
            'client': client,
            'venue': exchange,
            'account': account,
            'timestamp': {'$gte': item['timestamp'] - timedelta(days=1), '$lt': item['timestamp']}
          }))
          if len(leverages) > 0:
            avg_leverage = sum([lev['leverage'] for lev in leverages]) / len(leverages)
          else:
            avg_leverage = 1
          if avg_leverage == 0.0:
            avg_leverage = 1

          db['daily_returns'].update_one(
            {
              'client': client,
              'venue': exchange,
              'account': account,
              'timestamp': item['timestamp']
            },
            {'$set': {'avg_leverage': avg_leverage}}
          )