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
from src.handlers.instantiator import collect_borrow_rates
from src.handlers.instantiator import collect_funding_rates
from src.handlers.instantiator import collect_mark_prices
from src.handlers.instantiator import collect_fills
from src.handlers.instantiator import get_data_collectors
from src.config import read_config_file

config = read_config_file()

for client in config['clients']:
    data_collectors = get_data_collectors(client)

    for data_collector in data_collectors:
        collect_positions(client, data_collector)
        collect_instruments(client, data_collector)
        collect_mark_prices(client, data_collector)
        collect_tickers(client, data_collector)
        collect_borrow_rates(client, data_collector)
        collect_funding_rates(client, data_collector)
        collect_fills(client, data_collector)
        collect_balances(client, data_collector)
        collect_transactions(client, data_collector)
        collect_leverages(client, data_collector)


    # binance_subaccount1
    # collect_positions(client, data_collectors[1])
    # collect_instruments(client, data_collectors[1])
    # collect_mark_prices(client, data_collectors[1])
    # collect_tickers(client, data_collectors[1])
    # collect_borrow_rates(client, data_collectors[1])
    # collect_funding_rates(client, data_collectors[1])
    # collect_fills(client, data_collectors[1])
    # collect_balances(client, data_collectors[1])
    # collect_transactions(client, data_collectors[1])
    # collect_leverages(client, data_collectors[1])

    # binance_subaccount2
    # collect_positions(client, data_collectors[2]) 
    # collect_instruments(client, data_collectors[2]) 
    # collect_mark_prices(client, data_collectors[2])
    # collect_tickers(client, data_collectors[2])
    # collect_borrow_rates(client, data_collectors[2])
    # collect_funding_rates(client, data_collectors[2])
    # collect_fills(client, data_collectors[2])
    # collect_balances(client, data_collectors[2]) 
    # collect_transactions(client, data_collectors[2])
    # collect_leverages(client, data_collectors[2])

    # okx_subaccount1
    # collect_positions(client, data_collectors[3]) 
    # collect_instruments(client, data_collectors[3]) 
    # collect_mark_prices(client, data_collectors[3])
    # collect_tickers(client, data_collectors[3])
    # collect_borrow_rates(client, data_collectors[3])
    # collect_funding_rates(client, data_collectors[3])
    # collect_fills(client, data_collectors[3])
    # collect_balances(client, data_collectors[3])
    # collect_transactions(client, data_collectors[3])
    # collect_leverages(client, data_collectors[3])