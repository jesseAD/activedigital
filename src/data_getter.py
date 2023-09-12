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
        print(client + data_collector.exchange + data_collector.account)
        collect_positions(client, data_collector)
        print("collected position")
        collect_instruments(client, data_collector)
        print("collected instruments")
        collect_mark_prices(client, data_collector)
        print("collected mark price")
        collect_tickers(client, data_collector)
        print("collected tickers")
        collect_borrow_rates(client, data_collector)
        print("collected borrow rates")
        collect_funding_rates(client, data_collector)
        print("collected funding rates")
        collect_fills(client, data_collector)
        print("collected fills")
        collect_balances(client, data_collector)
        print("collected balances")
        collect_transactions(client, data_collector)
        print("collected transactions")
        collect_leverages(client, data_collector)
        print("collected leverages")


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