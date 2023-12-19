import os
import sys
import time
import gc
from dask.distributed import as_completed
from dask.distributed import wait
from dask.distributed import LocalCluster, Client
import concurrent.futures
import pymongo
from dotenv import load_dotenv
# import warnings

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

from src.lib.exchange import Exchange
from src.handlers.instantiator import positions_wrapper
from src.handlers.instantiator import balances_wrapper
from src.handlers.instantiator import instruments_wrapper
from src.handlers.instantiator import tickers_wrapper
from src.handlers.instantiator import index_prices_wrapper
from src.handlers.instantiator import leverages_wrapper
from src.handlers.instantiator import transactions_wrapper
from src.handlers.instantiator import borrow_rates_wrapper
from src.handlers.instantiator import funding_rates_wrapper
from src.handlers.instantiator import mark_prices_wrapper
from src.handlers.instantiator import fills_wrapper
from src.handlers.instantiator import bids_asks_wrapper
from src.handlers.instantiator import insert_runs
from src.handlers.instantiator import enclose_runs
from src.handlers.instantiator import get_data_collectors
from src.config import read_config_file
from lib.log import Log
from lib.db import MongoDB

config = read_config_file()
load_dotenv()
logger = Log()
# warnings.showwarning = logger.warning

mongo_uri = None
if os.getenv("mode") == "prod":
    mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

db = pymongo.MongoClient(mongo_uri, maxPoolsize=config['mongodb']['max_pool'])[config['mongodb']['database']]
# db = MongoDB(db_name=config['mongodb']['database'], uri=mongo_uri)

back_off = {}
exchs = {}

def public_pool(data_collectors, exchanges):
    db1 = pymongo.MongoClient(mongo_uri, maxPoolsize=config['mongodb']['max_pool'])[config['mongodb']['database']]
    executors = [concurrent.futures.ThreadPoolExecutor(config['dask']['threadsPerPool']) for i in range(config['dask']['threadPoolsPerWorker'])]
    
    threads = []
    for i in range(config['dask']['threadPoolsPerWorker']):
        for j in range(int(i * len(data_collectors) / config['dask']['threadPoolsPerWorker']), int((i+1) * len(data_collectors) / config['dask']['threadPoolsPerWorker'])):
            for exchange in exchanges:
                threads += (data_collectors[j](executors[i], exchs[exchange], exchange, logger, db1))
            pass
    
    for thread in concurrent.futures.as_completed(threads):
        print(thread.result())
        thread.cancel()

    db1.client.close()
    gc.collect()
    logger.info("Finished Public")

    return "Finished Public"

def private_pool(data_collectors, accounts_group):
    db2 = pymongo.MongoClient(mongo_uri, maxPoolsize=config['mongodb']['max_pool'])[config['mongodb']['database']]
    executors = [concurrent.futures.ThreadPoolExecutor(config['dask']['threadsPerPool']) for i in range(config['dask']['threadPoolsPerWorker'])]
    
    threads = []
    for i in range(config['dask']['threadPoolsPerWorker']):
        for j in range(int(i * len(accounts_group) / config['dask']['threadPoolsPerWorker']), int((i+1) * len(accounts_group) / config['dask']['threadPoolsPerWorker'])):
            for collector in data_collectors:
                threads.append(collector(executors[i], accounts_group[j].client, accounts_group[j], logger, db2))
    
    for thread in concurrent.futures.as_completed(threads):
        print(thread.result())
        thread.cancel()
    
    db2.client.close()
    gc.collect()   
    return "Finished Private"

def leverage_pool(leverage_collector, accounts_group):
    db3 = pymongo.MongoClient(mongo_uri, maxPoolsize=config['mongodb']['max_pool'])[config['mongodb']['database']]
    executors = [concurrent.futures.ThreadPoolExecutor(config['dask']['threadsPerPool']) for i in range(config['dask']['threadPoolsPerWorker'])]
    
    threads = []
    for i in range(config['dask']['threadPoolsPerWorker']):
        for j in range(int(i * len(accounts_group) / config['dask']['threadPoolsPerWorker']), int((i+1) * len(accounts_group) / config['dask']['threadPoolsPerWorker'])):
            threads.append(leverage_collector(executors[i], accounts_group[j].client, accounts_group[j], logger, db3))
    
    for thread in concurrent.futures.as_completed(threads):
        print(thread.result())
        thread.cancel()
    
    db3.client.close()
    gc.collect()
    return "Finished Leverage"


#   Insert new run
logger.info("Application started")
insert_runs(logger, db)
print("inserted a new run")

#  ------------  Dask + Concurrent  ----------------

public_data_collectors = [
    instruments_wrapper, tickers_wrapper, bids_asks_wrapper,
    funding_rates_wrapper, borrow_rates_wrapper,
    # mark_prices_wrapper, index_prices_wrapper
]

private_data_collectors = [
    balances_wrapper, positions_wrapper,
    fills_wrapper, transactions_wrapper
]

if config['dask']['workers'] > 0:
    cluster = LocalCluster(n_workers=config['dask']['workers'], memory_limit=config['dask']['memory'], processes=False)
    dask = Client(cluster)
    # futures = []

    # for exchange in config['exchanges']:
    #     exchs[exchange] = Exchange(exchange).exch()
    # #     back_off[exchange] = config['dask']['back_off']

    # for i in range(config['dask']['workers']):
    #     try:
    #         futures.append(dask.submit(
    #             public_pool, public_data_collectors, 
    #             config['exchanges'][int(len(config['exchanges']) / config['dask']['workers'] * i) : int(len(config['exchanges']) / config['dask']['workers'] * (i+1))]
    #         ))
    #     except Exception as e:
    #         print("Error in dask: ", e)


    # # wait(futures)
    # for done_work, result in as_completed(futures, with_results=True):
    #     print(result)
    #     dask.cancel(done_work) 

    # del futures
    # futures = []
    # exchs = {}

    # accounts = []
    # for client in config['clients']:
    #     data_collectors = get_data_collectors(client)
    #     accounts += data_collectors

    #     # for data_collector in data_collectors:
    #     #     back_off[client + "_" + data_collector.exchange + "_" + data_collector.account] = config['dask']['back_off']

    # for i in range(config['dask']['workers']):
    #     try:
    #         futures.append(dask.submit(
    #             private_pool, private_data_collectors, 
    #             accounts[int(len(accounts) / config['dask']['workers'] * i) : int(len(accounts) / config['dask']['workers'] * (i+1))]
    #         ))
    #     except Exception as e:
    #         print("Error in dask: ", e)

    # # wait(futures)
    # for done_work, result in as_completed(futures, with_results=True):
    #     print(result)
    #     dask.cancel(done_work) 
    # del futures
    # futures = []

    # for i in range(config['dask']['workers']):
    #     try:
    #         futures.append(dask.submit(
    #             leverage_pool, collect_leverages, 
    #             accounts[int(len(accounts) / config['dask']['workers'] * i) : int(len(accounts) / config['dask']['workers'] * (i+1))]
    #         ))
    #     except Exception as e:
    #         print("Error in dask: ", e)

    # # wait(futures)
    # for done_work, result in as_completed(futures, with_results=True):
    #     print(result)
    #     dask.cancel(done_work) 
    # del futures
    # del accounts


    dask.close()

else:
    for exchange in config['exchanges']:
        exchs[exchange] = Exchange(exchange).exch()

    public_pool(public_data_collectors, config['exchanges'])
    exchs = {}

    accounts = []
    for client in config['clients']:
        data_collectors = get_data_collectors(client)
        accounts += data_collectors

    private_pool(private_data_collectors, accounts)

    leverage_pool(leverages_wrapper, accounts)

    del accounts
    pass




#  ------------   parallelization with concurrent  -------------

# executors = { exchange: concurrent.futures.ThreadPoolExecutor(config['dask']['parallelization']) for exchange in config['exchanges']}

# futures = {exchange: [] for exchange in config['exchanges']}

# for exchange in config['exchanges']:
#     exch = Exchange(exchange).exch()
#     back_off[exchange] = config['dask']['back_off']

#     futures[exchange].append(executors[exchange].submit(collect_instruments, exch, exchange, back_off))
#     futures[exchange].append(executors[exchange].submit(collect_mark_prices, exch, exchange, back_off))
#     futures[exchange].append(executors[exchange].submit(collect_tickers, exch, exchange, back_off))
#     futures[exchange].append(executors[exchange].submit(collect_index_prices, exch, exchange, back_off))
#     futures[exchange].append(executors[exchange].submit(collect_funding_rates, exch, exchange, back_off))
#     futures[exchange].append(executors[exchange].submit(collect_borrow_rates, exch, exchange, back_off))

# for exchange in config['exchanges']:
#     for future in concurrent.futures.as_completed(futures[exchange]):
#         print(future.result())
#     del futures[exchange]
#     futures[exchange] = []

# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         back_off[client + "_" + data_collector.exchange + "_" + data_collector.account] = config['dask']['back_off']

#         futures[data_collector.exchange].append(
#             executors[data_collector.exchange].submit(collect_balances, client, data_collector, back_off)
#         )
#         futures[data_collector.exchange].append(
#             executors[data_collector.exchange].submit(collect_positions, client, data_collector, back_off)
#         )
#         futures[data_collector.exchange].append(
#             executors[data_collector.exchange].submit(collect_fills, client, data_collector, back_off)
#         )
#         futures[data_collector.exchange].append(
#             executors[data_collector.exchange].submit(collect_transactions, client, data_collector, back_off)
#         )

# for exchange in config['exchanges']:
#     for future in concurrent.futures.as_completed(futures[exchange]):
#         print(future.result())
#     del futures[exchange]
#     futures[exchange] = []

# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         futures[data_collector.exchange].append(
#             executors[data_collector.exchange].submit(collect_leverages, client, data_collector)
#         )

# for exchange in config['exchanges']:
#     for future in concurrent.futures.as_completed(futures[exchange]):
#         print(future.result())
#     del futures[exchange]
#     futures[exchange] = []









# --------------  Dask Parallelization  ----------------

# # Start a local Dask cluster
# cluster = LocalCluster(n_workers=config['dask']['parallelization'], memory_limit='500MB', processes=False)

# # Scale the cluster to 3 workers
# # cluster.scale(config['dask']['parallelization'])

# # Connect a client to the local cluster
# dask = Client(cluster)

#   Public Data
# futures1 = []
# for exchange in config['exchanges']:
#     back_off[exchange] = config['dask']['back_off']
#     exch = Exchange(exchange).exch()

#     future = dask.submit(collect_instruments, exch, exchange, back_off)
#     futures1.append(future)
#     # time.sleep(1)
#     # parallelization += 1
#     # if parallelization == config['dask']['parallelization']:
#     #     for done_work in as_completed(futures1, with_results=False):
#     #         # print(done_work.result())
#     #         print(done_work.status)
#     #         done_work.release()
#     #     parallelization = 0

#     future = dask.submit(collect_mark_prices, exch, exchange, back_off)
#     futures1.append(future)
#     # time.sleep(1)
#     # parallelization += 1
#     # if parallelization == config['dask']['parallelization']:
#     #     for done_work in as_completed(futures1, with_results=False):
#     #         # print(done_work.result())
#     #         print(done_work.status)
#     #         done_work.release()
#     #     parallelization = 0

#     future = dask.submit(collect_tickers, exch, exchange, back_off)
#     futures1.append(future)
#     # time.sleep(1)
#     # parallelization += 1
#     # if parallelization == config['dask']['parallelization']:
#     #     for done_work in as_completed(futures1, with_results=False):
#     #         # print(done_work.result())
#     #         print(done_work.status)
#     #         done_work.release()
#     #     parallelization = 0

#     future = dask.submit(collect_index_prices, exch, exchange, back_off)
#     futures1.append(future)
#     # time.sleep(1)
#     # parallelization += 1
#     # if parallelization == config['dask']['parallelization']:
#     #     for done_work in as_completed(futures1, with_results=False):
#     #         # print(done_work.result())
#     #         print(done_work.status)
#     #         done_work.release()
#     #     parallelization = 0

#     future = dask.submit(collect_funding_rates, exch, exchange, back_off)
#     futures1.append(future)
#     # time.sleep(1)
#     # parallelization += 1
#     # if parallelization == config['dask']['parallelization']:
#     #     for done_work in as_completed(futures1, with_results=False):
#     #         # print(done_work.result())
#     #         print(done_work.status)
#     #         done_work.release()
#     #     parallelization = 0

#     future = dask.submit(collect_borrow_rates, exch, exchange, back_off)
#     futures1.append(future)
#     # time.sleep(1)
#     # parallelization += 1
#     # if parallelization == config['dask']['parallelization']:
#     #     for done_work in as_completed(futures1, with_results=False):
#     #         # print(done_work.result())
#     #         print(done_work.status)
#     #         done_work.release()
#     #     parallelization = 0

# for done_work in as_completed(futures1, with_results=False):
#     print(done_work.result())
#     print(done_work.status)
#     done_work.release()
# del futures1
# parallelization = 0


#   Private Data
# futures2 = []
# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         back_off[client + "_" + data_collector.exchange + "_" + data_collector.account] = config['dask']['back_off']
        
#         future = dask.submit(collect_positions, client, data_collector, back_off)
#         futures2.append(future)
#         # time.sleep(1)
#         # parallelization += 1
#         # if parallelization == config['dask']['parallelization']:
#         #     wait(futures2)
            
#         #     for done_work in as_completed(futures2, with_results=False):
#         #         # print(done_work.result())
#         #         print(done_work.status)
#         #         done_work.release()
#         #     futures2 = []
#         #     parallelization = 0

#         future = dask.submit(collect_fills, client, data_collector, back_off)
#         futures2.append(future)
#         # time.sleep(1)
#         # parallelization += 1
#         # if parallelization == config['dask']['parallelization']:
#         #     wait(futures2)
#         #     for done_work in as_completed(futures2, with_results=False):
#         #         # print(done_work.result())
#         #         print(done_work.status)
#         #         done_work.release()
#         #     futures2 = []
#         #     parallelization = 0

#         future = dask.submit(collect_balances, client, data_collector, back_off)
#         futures2.append(future)
#         # time.sleep(1)
#         # parallelization += 1
#         # if parallelization == config['dask']['parallelization']:
#         #     wait(futures2)
#         #     for done_work in as_completed(futures2, with_results=False):
#         #         # print(done_work.result())
#         #         print(done_work.status)
#         #         done_work.release()
#         #     futures2 = []
#         #     parallelization = 0

#         future = dask.submit(collect_transactions, client, data_collector, back_off)
#         futures2.append(future)
#         # time.sleep(1)
#         # parallelization += 1
#         # if parallelization == config['dask']['parallelization']:
#         #     wait(futures2)
#         #     for done_work in as_completed(futures2, with_results=False):
#         #         # print(done_work.result())
#         #         print(done_work.status)
#         #         done_work.release()
#         #     futures2 = []
#         #     parallelization = 0

# for done_work in as_completed(futures2, with_results=False):
#     print(done_work.result())
#     print(done_work.status)
#     # done_work.release()
# del futures2
# parallelization = 0


# futures3 = []
# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         future = dask.submit(collect_leverages, client, data_collector)
#         futures3.append(future)
#         # time.sleep(1)
#         parallelization += 1
#         if parallelization == config['dask']['parallelization']:
#             wait(futures3)
#             for item in futures3:
#                 item.cancel()
#             futures3 = []
#             parallelization = 0
        
# wait(futures3)
# for item in futures3:
#     item.cancel()

# dask.close()







# ---------      Procedural     ------------

# Public Data
# for exchange in config['exchanges']:
#     exch = Exchange(exchange).exch()

#     collect_instruments(exch, exchange)
#     collect_mark_prices(exch, exchange)
#     collect_tickers(exch, exchange)
#     collect_index_prices(exch, exchange)
#     collect_funding_rates(exch, exchange)
#     collect_borrow_rates(exch, exchange)

# # Private Data
# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         collect_balances(client, data_collector)
#         collect_positions(client, data_collector)
#         collect_fills(client, data_collector)
#         collect_transactions(client, data_collector)

# # Leverage
# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         collect_leverages(client, data_collector)


enclose_runs(logger, db)
print("enclosed run")

logger.info("Application finished\n\n\n")
logger.zip_and_delete()

db.client.close()
gc.collect()
