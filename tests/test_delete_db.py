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

mongo_uri = 'mongodb+srv://activedigital:8EnNmGsai9pD0gxq@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client['active_digital']['leverages']

query = {'$and': [{'client': 'blackburn'}, {'venue': 'binance'}, {'account': 'submn'}]}

db.delete_many(query)