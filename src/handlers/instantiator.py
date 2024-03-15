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
from src.handlers.bids_asks import Bids_Asks
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
        client = client,
        exch = exch,
        exchange = exchange, 
        account = account,
    )
    
    return data_collector

def get_data_collectors(client):
    config = read_config_file()
    data_collectors = []

    collections = ['subaccounts', ]
    exchanges = ['binance', 'okx', 'bybit']

    for collection in collections:
        for exchange in config['clients'][client][collection]:
            if exchange not in exchanges:
                continue
            exchange_data = config['clients'][client][collection][exchange]
            for account in exchange_data:
                if account != "base_ccy":
                    data_collector = instantiate(client, collection, exchange, account)
                    data_collectors.append(data_collector)
         
    return data_collectors

#   Private data
def collect_positions(client_alias, data_collector, logger, db, balance_finished):
    res = False
    positions = Positions(db, 'positions')
    try:
        res = positions.create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            logger=logger,
            balance_finished = balance_finished
        )

    except Exception as e:
        logger.warning(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Positions " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Retrying Positions " + str(attempt))
            timeout *= 2

            try:
                res = positions.create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    logger=logger,
                    balance_finished = balance_finished
                )
            except Exception as e:
                logger.warning(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Positions " + str(e))

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        del positions

        if res:
            logger.info("Collected positions for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        else:
            logger.error("Unable to collect positions for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)

        return res

def collect_balances(client_alias, data_collector, logger, db, balance_finished):
    res = False
    balances = Balances(db, 'balances')
    try:
        res = balances.create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            logger=logger
        )

    except Exception as e:
        logger.warning(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Balances " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Retrying Balances " + str(attempt))
            timeout *= 2

            try:
                res = balances.create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        del balances

        if res:
            logger.info("Collected balances for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        else:
            logger.error("Unable to collect balances for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)

        balance_finished[client_alias + "_" + data_collector.exchange + "_" + data_collector.account] = True

        return res

def collect_transactions(client_alias, data_collector, logger, db):
    res = False
    transactions = Transactions(db, 'transactions')
    try:
        res = transactions.create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            symbol='BTCUSDT',
            logger=logger
        )

    except Exception as e:
        logger.warning(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Transactions " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Retrying Transactions " + str(attempt))
            timeout *= 2

            try:
                res = transactions.create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    symbol='BTCUSDT',
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        del transactions

        if res:
            logger.info("Collected transactions for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        else:
            logger.error("Unable to collect transactions for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)

        return res

def collect_fills(client_alias, data_collector, logger, db):
    res = False
    fills = Fills(db, 'fills')
    try:
        res = fills.create(
            client=client_alias,
            exch=data_collector.exch,
            exchange=data_collector.exchange,
            sub_account=data_collector.account,
            logger=logger
        )

    except Exception as e:
        logger.warning(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Fills " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][data_collector.exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(client_alias + " " + data_collector.exchange + " " + data_collector.account + " Retrying Fills " + str(attempt))
            timeout *= 2

            try:
                res = fills.create(
                    client=client_alias,
                    exch=data_collector.exch,
                    exchange=data_collector.exchange,
                    sub_account=data_collector.account,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][data_collector.exchange]['retry']:
                break
            attempt += 1

        del fills

        if res:
            logger.info("Collected fills for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        else:
            logger.error("Unable to collect fills for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)

        return res

#   Public data
def collect_instruments(exch, exchange, logger, db):
    res = False
    instruments = Instruments(db, 'instruments')
    try:
        res = instruments.create(
            exch=exch,
            exchange=exchange,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " Instruments " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Instruments " + str(attempt))
            timeout *= 2

            try:
                res = instruments.create(
                    exch=exch,
                    exchange=exchange,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        del instruments

        if res:
            logger.info("Collected instruments for " + exchange)
        else:
            logger.error("Unable to collect instruments for " + exchange)

        return res

def collect_tickers(exch, exchange, logger, db):
    res = False
    tickers = Tickers(db, 'tickers')
    try:
        res = tickers.create(
            exch=exch,
            exchange=exchange,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " Tickers " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Tickers " + str(attempt))
            timeout *= 2

            try:
                res = tickers.create(
                    exch=exch,
                    exchange=exchange,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1
        
        del tickers

        if res:
            logger.info("Collected tickers for " + exchange)
        else:
            logger.error("Unable to collect tickers for " + exchange)

        return res

def collect_index_prices(exch, exchange, symbols, logger, db):
    res = False
    index_prices = IndexPrices(db, 'index_prices')
    try:
        res = index_prices.create(
            exch=exch,
            exchange=exchange,
            symbols=symbols,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " Index Prices " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Index Prices " + str(attempt))
            timeout *= 2

            try:
                res = index_prices.create(
                    exch=exch,
                    exchange=exchange,
                    symbols=symbols,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        del index_prices

        if res:
            logger.info("Collected index prices for " + exchange)
        else:
            logger.error("Unable to collect index prices for " + exchange)
            
        return res

def collect_borrow_rates(exch, exchange, code, logger, db):
    res = False
    borrow_rates = BorrowRates(db, 'borrow_rates')
    try:
        res = borrow_rates.create(
            exch=exch,
            exchange=exchange,
            code=code,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " Borrow Rates " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Borrow Rates " + str(attempt))
            timeout *= 2

            try:
                res = borrow_rates.create(
                    exch=exch,
                    exchange=exchange,
                    code=code,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        del borrow_rates

        if res:
            logger.info("Collected borrow rates for " + exchange)
        else:
            logger.error("Unable to collect borrow rates for " + exchange)

        return res

def collect_funding_rates(exch, exchange, symbol, logger, db):
    res = False
    funding_rates = FundingRates(db, 'funding_rates')
    try:
        res = funding_rates.create(
            exch=exch,
            exchange=exchange,
            symbol=symbol,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " Funding Rates " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Funding Rates " + str(attempt))
            timeout *= 2

            try:
                res = funding_rates.create(
                    exch=exch,
                    exchange=exchange,
                    symbol=symbol,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        del funding_rates

        if res:
            logger.info("Collected funding rates for " + exchange)
        else:
            logger.error("Unable to collect funding rates for " + exchange)

        return res
    
def collect_bids_asks(exch, exchange, symbol, logger, db):
    res = False
    bid_asks = Bids_Asks(db, 'bid_asks')
    try:
        res = bid_asks.create(
            exch=exch,
            exchange=exchange,
            symbol=symbol,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " bids and asks " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Bids and Asks " + str(attempt))
            timeout *= 2

            try:
                res = bid_asks.create(
                    exch=exch,
                    exchange=exchange,
                    symbol=symbol,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        del bid_asks

        if res:
            logger.info("Collected bids and asks for " + exchange)
        else:
            logger.error("Unable to collect bids and asks for " + exchange)

        return res

def collect_mark_prices(exch, exchange, symbols, logger, db):
    res = False
    mark_prices = MarkPrices(db, 'mark_prices')
    try:
        res = mark_prices.create(
            exch=exch,
            exchange=exchange,
            symbols=symbols,
            logger=logger
        )

    except Exception as e:
        logger.warning(exchange + " mark prices " + str(e))

    finally:
        attempt = 1
        timeout = config['ccxt'][exchange]['timeout']

        while(not res):
            time.sleep(timeout / 1000)
            logger.info(exchange + " Retrying Mark Prices " + str(attempt))
            timeout *= 2

            try:
                res = mark_prices.create(
                    exch=exch,
                    exchange=exchange,
                    symbols=symbols,
                    logger=logger
                )
            except:
                pass

            if attempt == config['ccxt'][exchange]['retry']:
                break
            attempt += 1

        del mark_prices

        if res:
            logger.info("Collected mark prices for " + exchange)
        else:
            logger.error("Unable to collect mark prices for " + exchange)

        return res

def insert_runs(logger, db):
    return Runs(db, 'runs').start(logger)

def enclose_runs(logger, db):
    Runs(db, 'runs').end(logger)

def collect_leverages(client_alias, data_collector, logger, db):
    res = False
    leverages = Leverages(db, 'leverages')
    try:
        res = leverages.get(
            client=client_alias,
            exchange=data_collector.exchange,
            account=data_collector.account,
            logger=logger
        )
    finally:
        del leverages
        
        if res:
            logger.info("Collected leverage for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)
        else:
            logger.error("Unable to collect leverage for " + client_alias + " " + data_collector.exchange + " " + data_collector.account)

        return res

def instruments_wrapper(thread_pool, exch, exchange, symbols, logger, db):
    return [thread_pool.submit(collect_instruments, exch, exchange, logger, db)]

def tickers_wrapper(thread_pool, exch, exchange, symbols, logger, db):
    return [thread_pool.submit(collect_tickers, exch, exchange, logger, db)]

def funding_rates_wrapper(thread_pool, exch, exchange, symbols, logger, db):

    symbols = [symbol + "/USDT:USDT" for symbol in symbols] + [symbol + "/USD:" + symbol for symbol in symbols]

    threads = []
    for symbol in symbols:
        threads.append(thread_pool.submit(collect_funding_rates, exch, exchange, symbol, logger, db))

    return threads

def borrow_rates_wrapper(thread_pool, exch, exchange, symbols, logger, db):
    threads = []
    for code in symbols:
        threads.append(thread_pool.submit(collect_borrow_rates, exch, exchange, code, logger, db))

    return threads

def mark_prices_wrapper(thread_pool, exch, exchange, symbols, logger, db):
    threads = []
    if exchange == "bybit":
        for symbol in symbols:
            threads.append(thread_pool.submit(collect_mark_prices, exch, exchange, symbol, logger, db))
    else:
        threads.append(thread_pool.submit(collect_mark_prices, exch, exchange, symbols, logger, db))

    return threads

def index_prices_wrapper(thread_pool, exch, exchange, symbols, logger, db):
    threads = []
    if exchange == "bybit":
        for symbol in symbols:
            threads.append(thread_pool.submit(collect_index_prices, exch, exchange, symbol, logger, db))
    else:
        threads.append(thread_pool.submit(collect_index_prices, exch, exchange, symbols, logger, db))

    return threads

def bids_asks_wrapper(thread_pool, exch, exchange, symbols, logger, db):
    threads = []
    for symbol in symbols:
        threads.append(thread_pool.submit(collect_bids_asks, exch, exchange, symbol, logger, db))

    return threads

def positions_wrapper(thread_pool, client_alias, data_collector, logger, db, balance_finished):
    return thread_pool.submit(collect_positions, client_alias, data_collector, logger, db, balance_finished)

def balances_wrapper(thread_pool, client_alias, data_collector, logger, db, balance_finished):
    return thread_pool.submit(collect_balances, client_alias, data_collector, logger, db, balance_finished)

def transactions_wrapper(thread_pool, client_alias, data_collector, logger, db, balance_finished):
    return thread_pool.submit(collect_transactions, client_alias, data_collector, logger, db)

def fills_wrapper(thread_pool, client_alias, data_collector, logger, db, balance_finished):
    return thread_pool.submit(collect_fills, client_alias, data_collector, logger, db)

def leverages_wrapper(thread_pool, client_alias, data_collector, logger, db):
    return thread_pool.submit(collect_leverages, client_alias, data_collector, logger, db)