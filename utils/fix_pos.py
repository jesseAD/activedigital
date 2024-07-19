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


for client in config['clients']:
  for exchange in config['clients'][client]['subaccounts']:
    for account in config['clients'][client]['subaccounts'][exchange]:
      print(client + " " + exchange + " " + account)
      pos = list(db['positions'].aggregate([
        {'$match': {
          'client': client,
          'venue': exchange,
          'account': account,
          'runid': 69501
        }}
      ]))
      if len(pos) > 0:
        for runid in range(69502, 69573):
          print(runid)
          timestamp = list(db['runs'].find({'runid': runid}))[0]['start_time']

          db['positions'].update_many(
            {
              'client': client,
              'venue': exchange,
              'account': account,
              'runid': runid
            },
            {'$set': {
              'position_value': pos[0]['position_value'],
              'timestamp': timestamp,
              'alert_threshold': 1000,
              'active': True,
              'entry': False,
              'exit': False
            }},
            upsert=True
          )