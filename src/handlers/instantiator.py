import os, time

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
config = read_config_file()

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
    exchanges = ['binance', 'okx', 'bybit']

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
    res = False
    try:
        res = Positions('positions').create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Positions:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Positions " + str(attempt))
            timeout *= 2

            try:
                res = Positions('positions').create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        print("Collected positions for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        return res

def collect_balances(client_alias, data_collector, back_off={}):
    res = False
    try:
        res = Balances('balances').create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Balances:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Balances " + str(attempt))
            timeout *= 2

            try:
                res = Balances('balances').create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        print("Collected balances for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        return res

def collect_transactions(client_alias, data_collector, back_off={}):
    res = False
    try:
        res = Transactions('transactions').create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            symbol='BTCUSDT',
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Transactions:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Transactions " + str(attempt))
            timeout *= 2

            try:
                res = Transactions('transactions').create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        print("Collected transactions for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        return res

def collect_fills(client_alias, data_collector, back_off={}):
    res = False
    try:
        res = Fills('fills').create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Fills:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Fills " + str(attempt))
            timeout *= 2

            try:
                res = Fills('fills').create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        print("Collected fills for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        return res

#   Public data
def collect_instruments(exch, exchange, back_off={}):
    res = False
    try:
        res = Instruments('instruments').create(
            exch=exch,
            exchange=exchange,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Instruments:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Instruments " + str(attempt))
            timeout *= 2

            try:
                res = Instruments('instruments').create(
                    exch=exch,
                    exchange=exchange,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        print("Collected instruments for " + exchange)
        return res

def collect_tickers(exch, exchange, back_off={}):
    res = False
    try:
        res = Tickers('tickers').create(
            exch=exch,
            exchange=exchange,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Tickers:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Tickers " + str(attempt))
            timeout *= 2

            try:
                res = Tickers('tickers').create(
                    exch=exch,
                    exchange=exchange,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1
        
        print("Collected tickers for " + exchange)
        return res

def collect_index_prices(exch, exchange, back_off={}):
    res = False
    try:
        res = IndexPrices('index_prices').create(
            exch=exch,
            exchange=exchange,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Index Prices:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Index Prices " + str(attempt))
            timeout *= 2

            try:
                res = IndexPrices('index_prices').create(
                    exch=exch,
                    exchange=exchange,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        print("Collected index prices for " + exchange)
        return res

def collect_borrow_rates(exch, exchange, back_off={}):
    res = False
    try:
        res = BorrowRates('borrow_rates').create(
            exch=exch,
            exchange=exchange,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Borrow Rates:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Borrow Rates " + str(attempt))
            timeout *= 2

            try:
                res = BorrowRates('borrow_rates').create(
                    exch=exch,
                    exchange=exchange,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        print("Collected borrow rates for " + exchange)
        return res

def collect_funding_rates(exch, exchange, back_off={}):
    res = False
    try:
        res = FundingRates('funding_rates').create(
            exch=exch,
            exchange=exchange,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Funding Rates:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Funding Rates " + str(attempt))
            timeout *= 2

            try:
                res = FundingRates('funding_rates').create(
                    exch=exch,
                    exchange=exchange,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        print("Collected funding rates for " + exchange)
        return res

def collect_mark_prices(exch, exchange, back_off={}):
    res = False
    try:
        res = MarkPrices('mark_prices').create(
            exch=exch,
            exchange=exchange,
            back_off=back_off
        )

    except Exception as e:
        print("An error occurred in Mark Prices:", e)

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            print("Retrying Mark Prices " + str(attempt))
            timeout *= 2

            try:
                res = MarkPrices('mark_prices').create(
                    exch=exch,
                    exchange=exchange,
                    back_off=back_off
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        print("Collected mark prices for " + exchange)
        return res

def insert_runs():
    Runs('runs').start()

def enclose_runs():
    Runs('runs').end()

def collect_leverages(client_alias, data_collector):
    res = False
    try:
        res = Leverages('leverages').get(
            client=client_alias,
            exchange=data_collector.exchange,
            account=data_collector.account
        )
    finally:
        print("Collected leverage for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        return res