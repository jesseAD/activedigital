import os
import sys
import time
import gc
# from dask.distributed import as_completed
# from dask.distributed import wait
# from dask.distributed import LocalCluster, Client
import concurrent.futures

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

# # Start a local Dask cluster
# cluster = LocalCluster(n_workers=config['dask']['parallelization'], memory_limit='500MB', processes=False)

# # Scale the cluster to 3 workers
# # cluster.scale(config['dask']['parallelization'])

# # Connect a client to the local cluster
# dask = Client(cluster)

#   Insert new run
insert_runs()
print("inserted a new run")

# parallelization with concurrent

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

for exchange in config['exchanges']:
    exch = Exchange(exchange).exch()

    collect_instruments(exch, exchange)
    collect_mark_prices(exch, exchange)
    collect_tickers(exch, exchange)
    collect_index_prices(exch, exchange)
    collect_funding_rates(exch, exchange)
    collect_borrow_rates(exch, exchange)

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

for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        collect_balances(client, data_collector)
        collect_positions(client, data_collector)
        collect_fills(client, data_collector)
        collect_transactions(client, data_collector)


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

for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        collect_leverages(client, data_collector)

# dask.close()

enclose_runs()
print("enclosed run")