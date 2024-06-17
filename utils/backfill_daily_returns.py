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
db = pymongo.MongoClient(mongo_uri)

for client in config['clients']:
  for exchange in config['clients'][client]['subaccounts']:
    for account in config['clients'][client]['subaccounts'][exchange]:
      DailyReturns(db, "daily_returns").create(
        client=client,
        exchange=exchange,
        account=account,
        balance_finished={client + "_" + exchange + "_" + account: True}
      )