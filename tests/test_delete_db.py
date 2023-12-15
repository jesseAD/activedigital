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

num_workers = 2
num_pools = 1
num_threads = 2
num_repeat = 5
memory_limit = "250MB"
mongo_uri = 'mongodb+srv://activedigital:8EnNmGsai9pD0gxq@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client['active_digital']['lifetime_funding']

query = {'$and': [{'client': 'wizardly'}, {'venue': 'okx'}, {'account': 'submn1'}]}

db.delete_many(query)