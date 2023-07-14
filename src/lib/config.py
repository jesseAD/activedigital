import yaml

from data_collector import DataCollector
from src.handlers.positions import Positions

file_path = 'src/lib/config.yaml'
client_alias = 'deepspace'

def read_config_file(config_file_path):
    with open(config_file_path, 'r') as config_file:
        config_data = yaml.safe_load(config_file)
    
    return config_data

def instantiate(client, collection, exchange, account=None):
    config = read_config_file(file_path)
    target = config[client][collection][exchange]

    data_collector = DataCollector(
        config['mongo_host'],
        config['mongo_db'],
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
    config = read_config_file(file_path)
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

def run_script(data_collector):
    Positions.create(
        exchange=data_collector.exchange,
        positionType='long',
        sub_account=data_collector.account
    )

data_collectors = get_data_collectors(client_alias)

run_script(data_collectors[1]) # binance_subaccount1
run_script(data_collectors[2]) # binance_subaccount2
run_script(data_collectors[3]) # okk_subaccount1