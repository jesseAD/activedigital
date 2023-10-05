import os
import sys
import time
from dask.distributed import as_completed
from dask.distributed import wait
from dask.distributed import LocalCluster, Client

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

from src.lib.exchange import Exchange
from src.handlers.instantiator import collect_positions
from src.handlers.instantiator import collect_balances
from src.handlers.instantiator import collect_instruments
from src.handlers.instantiator import collect_tickers
from src.handlers.instantiator import collect_index_prices
from src.handlers.instantiator import collect_leverages
from src.handlers.instantiator import collect_transactions
from src.handlers.instantiator import collect_borrow_rates
from src.handlers.instantiator import collect_funding_rates
from src.handlers.instantiator import collect_mark_prices
from src.handlers.instantiator import collect_fills
from src.handlers.instantiator import insert_runs
from src.handlers.instantiator import enclose_runs
from src.handlers.instantiator import get_data_collectors
from src.config import read_config_file

config = read_config_file()
back_off = {}
parallelization = 0

# Start a local Dask cluster
# cluster = LocalCluster(n_workers=config['dask']['parallelization'], memory_limit='80MB', processes=False)

# Scale the cluster to 3 workers
# cluster.scale(config['dask']['parallelization'])

# Connect a client to the local cluster
# dask = Client(cluster)

#   Insert new run
insert_runs()
print("inserted a new run")

#   Public Data
# futures1 = []
# for exchange in config['exchanges']:
#     back_off[exchange] = config['dask']['back_off']
#     exch = Exchange(exchange).exch()

#     future = dask.submit(collect_instruments, exch, exchange, back_off)
#     futures1.append(future)
#     time.sleep(1)
#     parallelization += 1
#     if parallelization == config['dask']['parallelization']:
#         wait(futures1)
#         parallelization = 0

#     future = dask.submit(collect_mark_prices, exch, exchange, back_off)
#     futures1.append(future)
#     time.sleep(1)
#     parallelization += 1
#     if parallelization == config['dask']['parallelization']:
#         wait(futures1)
#         parallelization = 0

#     future = dask.submit(collect_tickers, exch, exchange, back_off)
#     futures1.append(future)
#     time.sleep(1)
#     parallelization += 1
#     if parallelization == config['dask']['parallelization']:
#         wait(futures1)
#         parallelization = 0

#     future = dask.submit(collect_index_prices, exch, exchange, back_off)
#     futures1.append(future)
#     time.sleep(1)
#     parallelization += 1
#     if parallelization == config['dask']['parallelization']:
#         wait(futures1)
#         parallelization = 0

#     future = dask.submit(collect_funding_rates, exch, exchange, back_off)
#     futures1.append(future)
#     time.sleep(1)
#     parallelization += 1
#     if parallelization == config['dask']['parallelization']:
#         wait(futures1)
#         parallelization = 0

#     future = dask.submit(collect_borrow_rates, exch, exchange, back_off)
#     futures1.append(future)
#     time.sleep(1)
#     parallelization += 1
#     if parallelization == config['dask']['parallelization']:
#         wait(futures1)
#         parallelization = 0

# wait(futures1)
# parallelization = 0

for exchange in config['exchanges']:
    exch = Exchange(exchange).exch()

    print(exchange)
    # collect_instruments(exch, exchange)
    # print("collected instruments")
    # collect_mark_prices(exch, exchange)
    # print("collected mark price")
    # collect_tickers(exch, exchange)
    # print("collected tickers")
    # collect_index_prices(exch, exchange)
    # print("collected index prices")
    # collect_funding_rates(exch, exchange)
    # print("collected funding rates")    
    # collect_borrow_rates(exch, exchange)
    # print("collected borrow rates")  

#   Private Data
# futures2 = []
# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         back_off[client + "_" + data_collector.exchange + "_" + data_collector.account] = config['dask']['back_off']
        
#         future = dask.submit(collect_positions, client, data_collector, back_off)
#         futures2.append(future)
#         time.sleep(1)
#         parallelization += 1
#         if parallelization == config['dask']['parallelization']:
#             wait(futures2)
#             parallelization = 0

#         future = dask.submit(collect_fills, client, data_collector, back_off)
#         futures2.append(future)
#         time.sleep(1)
#         parallelization += 1
#         if parallelization == config['dask']['parallelization']:
#             wait(futures2)
#             parallelization = 0

#         future = dask.submit(collect_balances, client, data_collector, back_off)
#         futures2.append(future)
#         time.sleep(1)
#         parallelization += 1
#         if parallelization == config['dask']['parallelization']:
#             wait(futures2)
#             parallelization = 0

#         future = dask.submit(collect_transactions, client, data_collector, back_off)
#         futures2.append(future)
#         time.sleep(1)
#         parallelization += 1
#         if parallelization == config['dask']['parallelization']:
#             wait(futures2)
#             parallelization = 0

# wait(futures2)
# parallelization = 0

for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        print(client + " " + data_collector.exchange + " " + data_collector.account)
        # collect_positions(client, data_collector)
        # print("collected position")        
        # collect_fills(client, data_collector)
        # print("collected fills")
        collect_balances(client, data_collector)
        print("collected balances")
        # collect_transactions(client, data_collector)
        # print("collected transactions")


# futures3 = []
# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         future = dask.submit(collect_leverages, client, data_collector)
#         futures3.append(future)
#         time.sleep(1)
#         parallelization += 1
#         if parallelization == config['dask']['parallelization']:
#             wait(futures3)
#             parallelization = 0
        
# wait(futures3)

for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        print(client + " " + data_collector.exchange + " " + data_collector.account)
        collect_leverages(client, data_collector)
        print("collected leverages")

# dask.close()

enclose_runs()
print("enclosed run")