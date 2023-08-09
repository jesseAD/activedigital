import os
import sys

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

from src.handlers.instantiator import collect_positions
from src.handlers.instantiator import collect_balances
from src.handlers.instantiator import collect_instruments
from src.handlers.instantiator import collect_tickers
from src.handlers.instantiator import collect_leverages
from src.handlers.instantiator import collect_transactions
from src.handlers.instantiator import get_data_collectors

client_lists = [
    'deepspace',
    'highsky'
]

for client in client_lists:
    data_collectors = get_data_collectors(client)

    # binance_subaccount1
    collect_positions(client, data_collectors[1])
    collect_balances(client, data_collectors[1])
    collect_instruments(client, data_collectors[1])
    collect_tickers(client, data_collectors[1])
    collect_transactions(client, data_collectors[1])
    collect_leverages(client, data_collectors[1])

    # binance_subaccount2
    collect_positions(client, data_collectors[2]) 
    collect_balances(client, data_collectors[2]) 
    collect_instruments(client, data_collectors[2]) 
    collect_tickers(client, data_collectors[2])
    collect_transactions(client, data_collectors[2])
    collect_leverages(client, data_collectors[2])

    # okx_subaccount1
    collect_positions(client, data_collectors[3]) 
    collect_balances(client, data_collectors[3])
    collect_instruments(client, data_collectors[3]) 
    collect_tickers(client, data_collectors[3])
    collect_transactions(client, data_collectors[3])
    collect_leverages(client, data_collectors[3])