import os
import sys

sys.path.append(os.getcwd())

from src.lib.data_collector import DataCollector
from src.handlers.positions import Positions
from src.handlers.balances import Balances
from src.handlers.instruments import Instruments
from src.handlers.tickers import Tickers
from src.handlers.leverages import Levarages
from src.lib.config import read_config_file

client_alias = 'deepspace'

def instantiate(client, collection, exchange, account=None):
    config = read_config_file()
    target = config[client][collection][exchange]

    data_collector = DataCollector(
        config['mongo_host'],
        config['mongo_port'],
        client,
        exchange, 
        collection,
        account,
        target['helper'],
        target[account]['apikey'],
        target[account]['apisecret'],
        target['function']
    )
    
    return data_collector

def get_data_collectors(client):
    config = read_config_file()
    data_collectors = []

    collections = ['funding_payments', 'balances']
    exchanges = ['bybit', 'binance', 'okx']

    for collection in collections:
        for exchange in config[client][collection]:
            if exchange not in exchanges:
                continue
            exchange_data = config[client][collection][exchange]
            for account in exchange_data:
                if account.startswith('subaccount'):
                    data_collector = instantiate(client, collection, exchange, account)
                    data_collectors.append(data_collector)
         
    return data_collectors

def collect_positions(data_collector):
    Positions('positions').create(
        exchange=data_collector.exchange,
        sub_account=data_collector.account
    )

def collect_balances(data_collector):
    Balances('balances').create(
        exchange=data_collector.exchange,
        sub_account=data_collector.account
    )

def collect_instruments(data_collector):
    Instruments('instruments').create(
        exchange=data_collector.exchange,
        sub_account=data_collector.account
    )

def collect_tickers(data_collector):
    Tickers('tickers').create(
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
        symbol='BTC/USDT'
    )

def collect_leverages(data_collector):
    Levarages('leverages').get(
        exchange=data_collector.exchange,
        account=data_collector.account
    )

# run in production mode
data_collectors = get_data_collectors(client_alias)

# # binance_subaccount1
collect_positions(data_collectors[1])
collect_balances(data_collectors[1])
collect_instruments(data_collectors[1])
collect_tickers(data_collectors[1])
collect_leverages(data_collectors[1])

# # binance_subaccount2
# collect_positions(data_collectors[2]) 
# collect_balances(data_collectors[2]) 
# collect_instruments(data_collectors[2]) 
# collect_tickers(data_collectors[2])

# okk_subaccount1
collect_positions(data_collectors[3]) 
collect_balances(data_collectors[3])
collect_instruments(data_collectors[3]) 
collect_tickers(data_collectors[3])
collect_leverages(data_collectors[3])