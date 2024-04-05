import os, sys
import ccxt
# import pymongo
# import pdb
from datetime import datetime, timezone
import time

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

# from src.handlers.positions import Positions
# from src.lib.log import Log

# logger = Log()

# mongo_uri = 'mongodb+srv://activedigital:'+'pwd'+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
# db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digital']

# params = {
#     'apiKey': "",
#     'secret': "",
#     'enableRateLimit': True,
#     'requests_trust_env':True,
#     'verbose': True,
#     'options': {
#         'adjustForTimeDifference':True,
#         'warnOnFetchOpenOrdersWithoutSymbol': False
#     },
#     'headers': {},
#     # 'password': "!"
# }

# exchange = ccxt.binance(params)
# print(exchange.fetch_borrow_rate_history())
os.chdir('/data/log/')
zip_files = os.listdir(os.getcwd())
for zip_file in zip_files:
    if zip_file[-3:] == "zip":
        current_date = datetime.now(timezone.utc)
        creation_date = datetime.strptime(zip_file, "%Y-%m-%d-%H-%M-%S.zip").replace(tzinfo=timezone.utc)

        print((current_date - creation_date).days)