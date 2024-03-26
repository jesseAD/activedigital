import os, sys
import ccxt
# import pymongo
# import pdb
from datetime import datetime, timezone
import time

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

# from src.handlers.positions import Positions
# from src.lib.log import Log

# logger = Log()

# mongo_uri = 'mongodb+srv://activedigital:'+'pwd'+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
# db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digital']

params = {
    'apiKey': "",
    'secret': "",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': True,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    # 'password': "pwd"
}

exchange = ccxt.bybit({'verbose': False})
print(exchange.fetch_ticker(symbol="BTCUSDM24"))
# print(exchange.fetch_open_interest(symbol="BTCUSDM24", params={'category': "inverse", 'intervalTime': "5min", 'limit': 1}))
# res = exchange.public_get_v5_market_kline(params={'category': 'linear', 'symbol': "OPUSDT", 'interval': 1, 'start': 1711086450000, 'end': 1711087290000, 'limit': 200})
# print(res['result']['list'])
# exchange.fetch_order_book(symbol="BTC/USD")
# exchange.fetch_order_book(symbol="BTC-USDT-240927")
# exchange.public_get_v5_market_tickers(params={'category': "linear", 'symbol': "BTC-22MAR24"})
# exchange.public_get_v5_market_tickers(params={'category': "inverse", 'symbol': "BTC-22MAR24"})
# exchange.fetch_order_book(symbol = "BTC/USDT")
# exchange.fapipublic_get_exchangeinfo()

# exchange = ccxt.binance(params)
# print("papi_get_balance")
# print(exchange.papi_get_balance())
# print("fetch_balance()")
# print(exchange.fetch_balance())

# exchange.private_get_mytrades()
# exchage = ccxt.bybit(params)


# exchage.fetch_my_trades(symbol="FILUSDT", since=1709804171353, params={'category': "linear", 'endTime': 1710408971353})
             

# print(exchange.papi_get_balance())
# print(exchange.fapiprivatev2_get_account())

# exchange.sapi_get_margin_tradecoeff()
# exchange.fapiprivatev2_get_balance()


# print(exchange.fetch_account_positions(params={"type": "future"}))

# exchange = ccxt.okx(params)

# transactions = []

# while(True):
#     end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
#     res = exchange.private_get_account_bills_archive(params={"end": end_time})["data"]

#     if len(res) == 0:
#         break

#     res.sort(key = lambda x: x['ts'])
#     transactions = res + transactions
#     print(res)
#     time.sleep(1)

# print(len(transactions))
    