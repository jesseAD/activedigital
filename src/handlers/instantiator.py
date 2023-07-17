from src.lib.data_collector import DataCollector
from src.handlers.positions import Positions
from src.lib.config import read_config_file

client_alias = 'deepspace'

def instantiate(mode, client, collection, exchange, account=None):
    config = read_config_file()
    target = config[client][collection][exchange]

    data_collector = DataCollector(
        config['mongo_host'],
        config['mongo_db_production'] if mode=='production' else config['mongo_db_test'],
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

def get_data_collectors(mode, client):
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
                    data_collector = instantiate(mode, client, collection, exchange, account)
                    data_collectors.append(data_collector)
         
    return data_collectors

def run_script(data_collector):
    Positions(data_collector.mongo_db).create(
        exchange=data_collector.exchange,
        positionType='long',
        sub_account=data_collector.account
    )

# run in production mode
data_collectors = get_data_collectors('production', client_alias)

run_script(data_collectors[1]) # binance_subaccount1
run_script(data_collectors[2]) # binance_subaccount2
run_script(data_collectors[3]) # okk_subaccount1Positions