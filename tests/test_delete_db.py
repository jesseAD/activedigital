from dask.distributed import as_completed
# from dask.distributed import wait
from dask.distributed import LocalCluster, Client
import concurrent.futures
import gc
import pymongo
import json
from datetime import datetime, timezone
import memory_profiler
import ccxt

mongo_uri = 'mongodb+srv://activedigital:pwd@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client['active_digital']['borrow_rates']

query = {'$and': [{'venue': 'binance'}, {'runid': 45299}]}
# query = {'$and': [{'client': 'lucid'}, {'venue': 'binance'}, {'account': 'subls1'}]}

db.delete_many(query)

# db.update_many(
#     {'runid': {'$lte': 45162}},
#     {'$set': {'market/vip': 'market'}}
# )