import os, sys
import ccxt
import pymongo
import pdb
from datetime import datetime, timezone
import time

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.positions import Positions
from src.lib.log import Log

logger = Log()

mongo_uri = 'mongodb+srv://activedigital:'+'pwd'+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digital']

params = {
    'apiKey': "api",
    'secret': "secret",
    'enableRateLimit': True,
    'requests_trust_env':True,
    'verbose': False,
    'options': {
        'adjustForTimeDifference':True,
    },
    'headers': {},
    # 'password': "pwd"
}

exchange = ccxt.binance(params)

positions = {"feeTier": "0", "canTrade": True, "canDeposit": True, "canWithdraw": True, "tradeGroupId": "-1", "updateTime": "0", "multiAssetsMargin": False, "totalInitialMargin": "248.39980834", "totalMaintMargin": "35.44205932", "totalWalletBalance": "98579.53556139", "totalUnrealizedProfit": "-39.29217535", "totalMarginBalance": "98540.24338604", "totalPositionInitialMargin": "248.39980834", "totalOpenOrderInitialMargin": "0.00000000", "totalCrossWalletBalance": "98579.53556139", "totalCrossUnPnl": "-39.29217535", "availableBalance": "98290.92545825", "maxWithdrawAmount": "98290.92545825", "assets": [{"asset": "BTC", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}, {"asset": "XRP", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}, {"asset": "TUSD", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}, {"asset": "BNB", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}, {"asset": "ETH", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}, {"asset": "USDT", "walletBalance": "98579.53556139", "unrealizedProfit": "-39.29217535", "marginBalance": "98540.24338604", "maintMargin": "35.44205932", "initialMargin": "248.39980834", "positionInitialMargin": "248.39980834", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "98290.92545825", "crossWalletBalance": "98579.53556139", "crossUnPnl": "-39.29217535", "availableBalance": "98290.92545825", "marginAvailable": True, "updateTime": "1707381929695"}, {"asset": "USDP", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}, {"asset": "USDC", "walletBalance": "0.00000000", "unrealizedProfit": "0.00000000", "marginBalance": "0.00000000", "maintMargin": "0.00000000", "initialMargin": "0.00000000", "positionInitialMargin": "0.00000000", "openOrderInitialMargin": "0.00000000", "maxWithdrawAmount": "0.00000000", "crossWalletBalance": "0.00000000", "crossUnPnl": "0.00000000", "availableBalance": "0.00000000", "marginAvailable": True, "updateTime": "0"}], "positions": [{'symbol': 'BNBUSDT', 'initialMargin': '20.49384999', 'maintMargin': '2.04938500', 'unrealizedProfit': '1.89840001', 'positionInitialMargin': '20.49384999', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '313.8296923077', 'breakEvenPrice': '313.8924582461', 'maxNotional': '500000', 'positionSide': 'BOTH', 'positionAmt': '1.30', 'notional': '409.87700000', 'isolatedWallet': '0', 'updateTime': '1707379207572', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'ETHUSDT_240329', 'initialMargin': '3.80107529', 'maintMargin': '0.76021505', 'unrealizedProfit': '-4.22220203', 'positionInitialMargin': '3.80107529', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '2316.106574542', 'breakEvenPrice': '-494.6223663484', 'maxNotional': '375000', 'positionSide': 'BOTH', 'positionAmt': '-0.031', 'notional': '-76.02150584', 'isolatedWallet': '0', 'updateTime': '1707363389905', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'OPUSDT', 'initialMargin': '27.54927778', 'maintMargin': '3.30591333', 'unrealizedProfit': '-12.42073332', 'positionInitialMargin': '27.54927778', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '3.429131405965', 'breakEvenPrice': '3.429817232246', 'maxNotional': '600000', 'positionSide': 'BOTH', 'positionAmt': '164.3', 'notional': '550.98555569', 'isolatedWallet': '0', 'updateTime': '1707367538195', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'BTCUSDT_240329', 'initialMargin': '56.57280159', 'maintMargin': '11.31456031', 'unrealizedProfit': '20.05203191', 'positionInitialMargin': '56.57280159', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '44456.16', 'breakEvenPrice': '44474.1250416', 'maxNotional': '375000', 'positionSide': 'BOTH', 'positionAmt': '0.025', 'notional': '1131.45603191', 'isolatedWallet': '0', 'updateTime': '1707376843335', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'ATOMUSDT', 'initialMargin': '2.32086724', 'maintMargin': '0.27850406', 'unrealizedProfit': '-2.13300808', 'positionInitialMargin': '2.32086724', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '9.093293008726', 'breakEvenPrice': '-0.0164270856344', 'maxNotional': '600000', 'positionSide': 'BOTH', 'positionAmt': '-4.87', 'notional': '-46.41734499', 'isolatedWallet': '0', 'updateTime': '1707380104407', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'SOLUSDT', 'initialMargin': '15.25133016', 'maintMargin': '1.52513301', 'unrealizedProfit': '0.00760335', 'positionInitialMargin': '15.25133016', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '101.673', 'breakEvenPrice': '101.6933346', 'maxNotional': '2000000', 'positionSide': 'BOTH', 'positionAmt': '3', 'notional': '305.02660335', 'isolatedWallet': '0', 'updateTime': '1707372914022', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'XRPUSDT', 'initialMargin': '35.40910474', 'maintMargin': '3.54091047', 'unrealizedProfit': '-8.00044816', 'positionInitialMargin': '35.40910474', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '0.506900495499', 'breakEvenPrice': '0.4869575539602', 'maxNotional': '750000', 'positionSide': 'BOTH', 'positionAmt': '-1381.3', 'notional': '-708.18209499', 'isolatedWallet': '0', 'updateTime': '1707381929695', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'DOTUSDT', 'initialMargin': '5.04032220', 'maintMargin': '0.65524188', 'unrealizedProfit': '-3.75027753', 'positionInitialMargin': '5.04032220', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '6.693528730159', 'breakEvenPrice': '5.636164540688', 'maxNotional': '500000', 'positionSide': 'BOTH', 'positionAmt': '-14.5', 'notional': '-100.80644412', 'isolatedWallet': '0', 'updateTime': '1707376501979', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'ADAUSDT', 'initialMargin': '20.17994636', 'maintMargin': '2.01799463', 'unrealizedProfit': '-22.14976324', 'positionInitialMargin': '20.17994636', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '0.4979754175017', 'breakEvenPrice': '0.4783362120103', 'maxNotional': '250000', 'positionSide': 'BOTH', 'positionAmt': '-766', 'notional': '-403.59892730', 'isolatedWallet': '0', 'updateTime': '1707380106159', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'ARBUSDT', 'initialMargin': '1.37912010', 'maintMargin': '0.16549441', 'unrealizedProfit': '-0.14591794', 'positionInitialMargin': '1.37912010', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '1.8992', 'breakEvenPrice': '1.89957984', 'maxNotional': '600000', 'positionSide': 'BOTH', 'positionAmt': '14.6', 'notional': '27.58240205', 'isolatedWallet': '0', 'updateTime': '1707367509332', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'AVAXUSDT', 'initialMargin': '1.77564558', 'maintMargin': '0.17756455', 'unrealizedProfit': '-0.11691174', 'positionInitialMargin': '1.77564558', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '35.396', 'breakEvenPrice': '35.5552484', 'maxNotional': '250000', 'positionSide': 'BOTH', 'positionAmt': '-1', 'notional': '-35.51291174', 'isolatedWallet': '0', 'updateTime': '1707354920741', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'MATICUSDT', 'initialMargin': '25.92688543', 'maintMargin': '3.11122625', 'unrealizedProfit': '-4.17048660', 'positionInitialMargin': '25.92688543', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '0.8390179775281', 'breakEvenPrice': '0.8391857811236', 'maxNotional': '900000', 'positionSide': 'BOTH', 'positionAmt': '623', 'notional': '518.53770871', 'isolatedWallet': '0', 'updateTime': '1707368441154', 'bidNotional': '0', 'askNotional': '0'}, {'symbol': 'DYDXUSDT', 'initialMargin': '32.69958188', 'maintMargin': '6.53991637', 'unrealizedProfit': '-4.14046198', 'positionInitialMargin': '32.69958188', 'openOrderInitialMargin': '0', 'leverage': '20', 'isolated': False, 'entryPrice': '2.87393930131', 'breakEvenPrice': '2.87451408917', 'maxNotional': '400000', 'positionSide': 'BOTH', 'positionAmt': '229.0', 'notional': '653.99163772', 'isolatedWallet': '0', 'updateTime': '1707366605336', 'bidNotional': '0', 'askNotional': '0'}]}

pdb.set_trace()
parsed = exchange.parse_account_positions(positions)
print(parsed)

             

# print(exchange.papi_get_balance())
# print(exchange.fapiprivatev2_get_account())

# exchange.sapi_get_margin_tradecoeff()
# exchange.fapiprivatev2_get_balance()


# print(exchange.fetch_account_positions(params={"type": "future"}))

exchange = ccxt.okx(params)

transactions = []

while(True):
    end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
    res = exchange.private_get_account_bills_archive(params={"end": end_time})["data"]

    if len(res) == 0:
        break

    res.sort(key = lambda x: x['ts'])
    transactions = res + transactions
    print(res)
    time.sleep(1)
    