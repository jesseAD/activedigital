from src.lib.data_collector import DataCollector
from src.handlers.positions import Positions
from src.handlers.balances import Balances
from src.handlers.instruments import Instruments
from src.handlers.tickers import Tickers
from src.handlers.index_prices import IndexPrices
from src.handlers.leverages import Leverages
from src.handlers.transactions import Transactions
from src.handlers.borrow_rates import BorrowRates
from src.handlers.funding_rates import FundingRates
from src.handlers.mark_price import MarkPrices
from src.handlers.fills import Fills
from src.handlers.runs import Runs
from src.config import read_config_file

def instantiate(client, collection, exchange, account=None):
    config = read_config_file()
    target = config['clients'][client][collection][exchange]

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

    collections = ['funding_payments', ]
    exchanges = ['binance', 'okx']

    for collection in collections:
        for exchange in config['clients'][client][collection]:
            if exchange not in exchanges:
                continue
            exchange_data = config['clients'][client][collection][exchange]
            for account in exchange_data:
                if account.startswith('sub'):
                    data_collector = instantiate(client, collection, exchange, account)
                    data_collectors.append(data_collector)
         
    return data_collectors

def collect_positions(client_alias, data_collector):
    Positions('positions').create(
        client=client_alias,
        exchange=data_collector.exchange,
        sub_account=data_collector.account
    )

def collect_balances(client_alias, data_collector):
    Balances('balances').create(
        client=client_alias,
        exchange=data_collector.exchange,
        sub_account=data_collector.account
    )

def collect_instruments(exchange):
    Instruments('instruments').create(
        exchange=exchange
    )

def collect_tickers(exchange):
    Tickers('tickers').create(
        exchange=exchange,
    )

def collect_index_prices(exchange):
    IndexPrices('index_prices').create(
        exchange=exchange,
    )

def collect_leverages(client_alias, data_collector):
    Leverages('leverages').get(
        client=client_alias,
        exchange=data_collector.exchange,
        account=data_collector.account
    )

def collect_transactions(client_alias, data_collector):
    Transactions('transactions').create(
        client=client_alias,
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
        symbol='BTCUSDT',
    )

def collect_borrow_rates(exchange):
    BorrowRates('borrow_rates').create(
        exchange=exchange,
    )

def collect_funding_rates(exchange):
    FundingRates('funding_rates').create(
        exchange=exchange,
    )

def collect_mark_prices(exchange):
    MarkPrices('mark_prices').create(
        exchange=exchange,
    )

def collect_fills(client_alias, data_collector):
    Fills('fills').create(
        client=client_alias,
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
    )

def insert_runs():
    Runs('runs').start()

def enclose_runs():
    Runs('runs').end()