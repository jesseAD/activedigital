import yaml
import subprocess

from data_collector import DataCollector

file_path = 'config.yaml'
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
    subprocess.run(['python', data_collector.script])

get_data_collectors(client_alias)