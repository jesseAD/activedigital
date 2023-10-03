import os

from src.lib.exchange import Exchange
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
from dotenv import load_dotenv

load_dotenv()

def instantiate(client, collection, exchange, account=None):
    config = read_config_file()
    target = config['clients'][client][collection][exchange]

    spec = client.upper() + "_" + exchange.upper() + "_" + account.upper() + "_"
    API_KEY = os.getenv(spec + "API_KEY")
    API_SECRET = os.getenv(spec + "API_SECRET")
    PASSPHRASE = None
    if exchange == "okx":
        PASSPHRASE = os.getenv(spec + "PASSPHRASE")

    exch = Exchange(exchange, account, API_KEY, API_SECRET, PASSPHRASE).exch()

    data_collector = DataCollector(
        mongo_host = config['mongo_host'],
        mongo_port = config['mongo_port'],
        client = client,
        exch = exch,
        exchange = exchange, 
        collection = collection,
        account = account,
        helper = target['helper'],
        apikey = target[account]['apikey'],
        apisecret = target[account]['apisecret'],
        script = target['function']
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

#   Private data
def collect_positions(client_alias, data_collector, back_off={}):
    positions = Positions('positions')
    positions.create(
        client=client_alias,
        exch=data_collector.exch,
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
        back_off=back_off
    )
    del positions

def collect_balances(client_alias, data_collector, back_off={}):
    balances = Balances('balances')
    balances.create(
        client=client_alias,
        exch=data_collector.exch,
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
        back_off=back_off
    )
    del balances

def collect_transactions(client_alias, data_collector, back_off={}):
    transactions = Transactions('transactions')
    transactions.create(
        client=client_alias,
        exch=data_collector.exch,
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
        symbol='BTCUSDT',
        back_off=back_off
    )
    del transactions

def collect_fills(client_alias, data_collector, back_off={}):
    fills = Fills('fills')
    fills.create(
        client=client_alias,
        exch=data_collector.exch,
        exchange=data_collector.exchange,
        sub_account=data_collector.account,
        back_off=back_off
    )
    del fills

#   Public data
def collect_instruments(exch, exchange, back_off={}):
    instruments = Instruments('instruments')
    instruments.create(
        exch=exch,
        exchange=exchange,
        back_off=back_off
    )
    del instruments

def collect_tickers(exch, exchange, back_off={}):
    ticker = Tickers('tickers')
    ticker.create(
        exch=exch,
        exchange=exchange,
        back_off=back_off
    )
    del ticker

def collect_index_prices(exch, exchange, back_off={}):
    index_prices = IndexPrices('index_prices')
    index_prices.create(
        exch=exch,
        exchange=exchange,
        back_off=back_off
    )
    del index_prices

def collect_borrow_rates(exch, exchange, back_off={}):
    borrow_rates = BorrowRates('borrow_rates')
    borrow_rates.create(
        exch=exch,
        exchange=exchange,
        back_off=back_off
    )
    del borrow_rates

def collect_funding_rates(exch, exchange, back_off={}):
    funding_rates = FundingRates('funding_rates')
    funding_rates.create(
        exch=exch,
        exchange=exchange,
        back_off=back_off
    )
    del funding_rates

def collect_mark_prices(exch, exchange, back_off={}):
    mark_prices = MarkPrices('mark_prices')
    mark_prices.create(
        exch=exch,
        exchange=exchange,
        back_off=back_off
    )
    del mark_prices

def insert_runs():
    Runs('runs').start()

def enclose_runs():
    Runs('runs').end()

def collect_leverages(client_alias, data_collector):
    Leverages('leverages').get(
        client=client_alias,
        exchange=data_collector.exchange,
        account=data_collector.account
    )