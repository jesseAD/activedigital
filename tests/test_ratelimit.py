import os
import sys
import time
import gc
from dask.distributed import as_completed
from dask.distributed import wait
from dask.distributed import LocalCluster, Client
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

# back_off = {"binance": config['dask']['back_off']}
# exch = Exchange("binance").exch()

back_off = {"blackburn_binance_submn1": config['dask']['back_off']}
data_collectors = get_data_collectors("blackburn")

executor = concurrent.futures.ThreadPoolExecutor(256)

threads = []
for i in range(150):
    threads.append(executor.submit(collect_positions, "blackburn", data_collectors[0], back_off))
    # threads.append(executor.submit(collect_funding_rates, exch, "binance", back_off))

for thread in concurrent.futures.as_completed(threads):
    print(thread.result())
    thread.cancel()