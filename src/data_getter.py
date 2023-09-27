import os
import sys
from dask.distributed import Client
from dask.distributed import as_completed
from dask.distributed import wait

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
dask = Client(processes=False)
back_off = {}

#   Insert new run
insert_runs()
print("inserted a new run")

#   Public Data
futures1 = []
for exchange in config['exchanges']:
    back_off[exchange] = config['back_off']
    exch = Exchange(exchange).exch()

    future = dask.submit(collect_instruments, exch, exchange, back_off)
    futures1.append(future)
    future = dask.submit(collect_mark_prices, exch, exchange, back_off)
    futures1.append(future)
    future = dask.submit(collect_tickers, exch, exchange, back_off)
    futures1.append(future)
    future = dask.submit(collect_index_prices, exch, exchange, back_off)
    futures1.append(future)
    future = dask.submit(collect_funding_rates, exch, exchange, back_off)
    futures1.append(future)
    future = dask.submit(collect_borrow_rates, exch, exchange, back_off)
    futures1.append(future)

wait(futures1)

# for exchange in config['exchanges']:
#     print(exchange)
#     collect_instruments(exchange)
#     print("collected instruments")
#     collect_mark_prices(exchange)
#     print("collected mark price")
#     collect_tickers(exchange)
#     print("collected tickers")
#     collect_index_prices(exchange)
#     print("collected index prices")
#     collect_funding_rates(exchange)
#     print("collected funding rates")    
#     collect_borrow_rates(exchange)
#     print("collected borrow rates")  

#   Private Data
futures2 = []
for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        back_off[client + "_" + data_collector.exchange + "_" + data_collector.account] = config['back_off']
        
        future = dask.submit(collect_positions, client, data_collector, back_off)
        futures2.append(future)
        future = dask.submit(collect_fills, client, data_collector, back_off)
        futures2.append(future)
        future = dask.submit(collect_balances, client, data_collector, back_off)
        futures2.append(future)
        future = dask.submit(collect_transactions, client, data_collector, back_off)
        futures2.append(future)

wait(futures2)

# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         print(client + " " + data_collector.exchange + " " + data_collector.account)
#         collect_positions(client, data_collector)
#         print("collected position")        
#         collect_fills(client, data_collector)
#         print("collected fills")
#         collect_balances(client, data_collector)
#         print("collected balances")
#         collect_transactions(client, data_collector)
#         print("collected transactions")


futures3 = []
for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        future = dask.submit(collect_leverages, client, data_collector)
        futures3.append(future)
        
wait(futures3)

# for client in config['clients']:
#     data_collectors = get_data_collectors(client)

#     for data_collector in data_collectors:
#         print(client + " " + data_collector.exchange + " " + data_collector.account)
#         collect_leverages(client, data_collector)
#         print("collected leverages")

enclose_runs()
print("enclosed run")