from dask.distributed import as_completed
# from dask.distributed import wait
from dask.distributed import LocalCluster, Client
import concurrent.futures
import gc
# import pymongo
# import json
# from datetime import datetime, timezone
import memory_profiler
# import ccxt

num_workers = 1
num_pools = 1
num_threads = 1
memory_limit = "1000MB"
num_repeat = 50000

# @memory_profiler.profile
def persist_to_db(mongo_client, exchange):
    for i in range(num_repeat):
        # db = mongo_client['active_digital']['tickers']

        # with open('tests/sesa.json') as sesa:
        #     tickers = json.load(sesa)['tickers']

        # tickers = exchange.fetch_tickers()

        # ticker = {
        #     "client": "client",
        #     "venue": "exchange",
        #     "account": "Main Account",
        #     "ticker_value": tickers,
        #     "active": True,
        #     "entry": False,
        #     "exit": False,
        #     "timestamp": datetime.now(timezone.utc),
        #     "runid": 9999
        # }

        # db.insert_one(ticker)

        # del ticker
        # del tickers
        for j in range(i):
            temp = j * i

        # gc.collect()
    
    return "Finished Thread"

# @memory_profiler.profile
def thread_pool(mongo_uri, maxPoolSize, exchange, i):
    # mongo_client = pymongo.MongoClient(mongo_uri, maxPoolsize=maxPoolSize)
    mongo_client = None

    executors = [concurrent.futures.ThreadPoolExecutor(num_threads) for i in range(num_pools)]

    threads = []
    for i in range(num_pools):
        for j in range(num_threads):
            threads.append(executors[i].submit(persist_to_db, mongo_client, exchange))

    for thread in concurrent.futures.as_completed(threads):
        print(thread.result())
        thread.cancel()

    # mongo_client.close()
    gc.collect()
    
    return "Finished ThreadPool"

# @memory_profiler.profile
def run_dask():
    # exchange = ccxt.binance({
    #     'enableRateLimit': True,
    #     'requests_trust_env':True,
    #     'verbose': False,
    #     'options': {
    #         'adjustForTimeDifference':True,
    #     }
    # })
    exchange = None

    cluster = LocalCluster(n_workers=num_workers, memory_limit=memory_limit, processes=False)
    dask = Client(cluster)
    futures = []

    for i in range(num_workers):
        try:
            futures.append(dask.submit(thread_pool, None, 1, exchange, i))
        except Exception as e:
            print("Error in submitting dask futures: ", e)

    for done, res in as_completed(futures=futures, with_results=True):
        print(res)
        dask.cancel(done)

    dask.close()

run_dask()