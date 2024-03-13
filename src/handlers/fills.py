import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
from src.lib.mapping import Mapping

load_dotenv()
config = read_config_file()


class Fills:
    def __init__(self, db, collection):

        self.runs_db = db['runs']
        self.positions_db = db['positions']
        self.fills_db = db['fills']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     exchange: str = None,
    #     account: str = None,
    # ):
    #     results = []

    #     pipeline = [
    #         {"$sort": {"_id": -1}},
    #     ]

    #     if active is not None:
    #         pipeline.append({"$match": {"active": active}})
    #     if spot:
    #         pipeline.append({"$match": {"spotMarket": spot}})
    #     if future:
    #         pipeline.append({"$match": {"futureMarket": future}})
    #     if perp:
    #         pipeline.append({"$match": {"perpMarket": perp}})
    #     if exchange:
    #         pipeline.append({"$match": {"venue": exchange}})
    #     if account:
    #         pipeline.append({"$match": {"account": account}})

    #     try:
    #         results = self.fills_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

    def create(
        self,
        client,
        exch=None,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        fillsValue: str = None,
        symbols: str = None,
        back_off = {},
        logger=None
    ):
        if symbols is None:
            # get latest positions data
            query = {}
            if client:
                query["client"] = client
            if exchange:
                query["venue"] = exchange
            if sub_account:
                query["account"] = sub_account

            position_values = self.positions_db.aggregate([
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$client', client
                                    ]
                                }, {
                                    '$eq': [
                                        '$venue', exchange
                                    ]
                                }, {
                                    '$eq': [
                                        '$account', sub_account
                                    ]
                                }, {
                                    '$gt': [
                                        '$timestamp', datetime.now(timezone.utc) - timedelta(days=1)
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'position_value': 1
                    }
                }, {
                    '$group': {
                        '_id': None, 
                        'position_value': {
                            '$last': '$position_value'
                        }
                    }
                }
            ])

            symbols = []
            for position in position_values:
                for item in position['position_value']:
                    symbols.append(item['info']['symbol'])

        if fillsValue is None:
            if exch == None:
                spec = client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_"
                API_KEY = os.getenv(spec + "API_KEY")
                API_SECRET = os.getenv(spec + "API_SECRET")
                PASSPHRASE = None
                if exchange == "okx":
                    PASSPHRASE = os.getenv(spec + "PASSPHRASE")

                exch = Exchange(exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE).exch()

            fillsValue = {}

            for symbol in symbols:
                query = {}
                if client:
                    query["client"] = client
                if exchange:
                    query["venue"] = exchange
                if sub_account:
                    query["account"] = sub_account
                query["symbol"] = symbol

                fills_values = self.fills_db.aggregate([
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$client', client
                                        ]
                                    }, {
                                        '$eq': [
                                            '$venue', exchange
                                        ]
                                    }, {
                                        '$eq': [
                                            '$account', sub_account
                                        ]
                                    }, {
                                        '$eq': [
                                            '$symbol', symbol
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'fills_value': 1
                        }
                    }, {
                        '$sort': {'fills_value.timestamp': 1}
                    },
                    {
                        '$group': {
                            '_id': None, 
                            'fills_value': {
                                '$last': '$fills_value'
                            }
                        }
                    }
                ])

                current_value = None
                for item in fills_values:
                    current_value = item["fills_value"]

                try:
                    if current_value is None:
                        if exchange == "okx":
                            fillsValue[symbol] = OKXHelper().get_fills(exch=exch, symbol=symbol, limit=100)
                            # fillsValue[symbol] = Mapping().mapping_fills(
                            #     exchange=exchange,
                            #     fills=OKXHelper().get_fills(
                            #         exch=exch,
                            #         params={
                            #             "instType": "SWAP",
                            #             "instId": symbol,
                            #             "limit": 100,
                            #         },
                            #     ),
                            # )
                        elif exchange == "bybit":
                            fillsValue[symbol] = BybitHelper().get_fills(exch=exch, symbol=symbol)
                        elif exchange == "binance":
                            if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
                                fills = Helper().get_pm_fills(exch=exch, symbol=symbol, params={'limit': 100})
                                for item in fills:
                                    item['info'] = {**item}
                                    item['takerOrMaker'] = "maker" if item['maker'] else "taker"

                                fillsValue[symbol] = Mapping().mapping_fills(
                                    exchange=exchange,
                                    fills=fills
                                )
                            else:
                                fillsValue[symbol] = Helper().get_fills(exch=exch, symbol=symbol, limit=100)
                                # fillsValue[symbol] = Mapping().mapping_fills(
                                #     exchange=exchange,
                                #     fills=Helper().get_fills(
                                #         exch=exch, params={"symbol": symbol, "limit": 100}
                                #     ),
                                # )
                    else:
                        if config["fills"]["fetch_type"] == "time":
                            last_time = int(current_value["timestamp"]) + 1
                            if exchange == "okx":
                                fillsValue[symbol] = OKXHelper().get_fills(exch=exch, symbol=symbol, limit=100, since=last_time)
                                # fillsValue[symbol] = Mapping().mapping_fills(
                                #     exchange=exchange,
                                #     fills=OKXHelper().get_fills(
                                #         exch=exch,
                                #         params={
                                #             "instType": "SWAP",
                                #             "instId": symbol,
                                #             "limit": 100,
                                #             "begin": last_time,
                                #         },
                                #     ),
                                # )
                            if exchange == "bybit":
                                fillsValue[symbol] = BybitHelper().get_fills(
                                    exch=exch, symbol=symbol, since=last_time,
                                    params={'endTime': datetime.now(timezone.utc).timestamp() * 1000}
                                )
                            elif exchange == "binance":
                                if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
                                    fills = Helper().get_pm_fills(exch=exch, symbol=symbol, params={'limit': 100, 'startTime': last_time})
                                    for item in fills:
                                        item['info'] = {**item}
                                        item['takerOrMaker'] = "maker" if item['maker'] else "taker"

                                    fillsValue[symbol] = Mapping().mapping_fills(
                                        exchange=exchange,
                                        fills=fills
                                    )
                                else:
                                    fillsValue[symbol] = Helper().get_fills(exch=exch, symbol=symbol, limit=100, since=last_time)
                                # fillsValue[symbol] = Mapping().mapping_fills(
                                #     exchange=exchange,
                                #     fills=Helper().get_fills(
                                #         exch=exch,
                                #         params={
                                #             "symbol": symbol,
                                #             "limit": 100,
                                #             "startTime": last_time,
                                #         },
                                #     ),
                                # )
                        elif config["fills"]["fetch_type"] == "id":
                            last_id = int(current_value["id"]) + 1
                            if exchange == "okx":
                                fillsValue[symbol] = OKXHelper().get_fills(exch=exch, symbol=symbol, limit=100, params={'before': last_id})
                                # fillsValue[symbol] = Mapping().mapping_fills(
                                #     exchange=exchange,
                                #     fills=OKXHelper().get_fills(
                                #         exch=exch,
                                #         params={
                                #             "instType": "SWAP",
                                #             "instId": symbol,
                                #             "limit": 100,
                                #             "before": last_id,
                                #         },
                                #     ),
                                # )
                            elif exchange == "binance":
                                if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
                                    fills = Helper().get_pm_fills(exch=exch, symbol=symbol, params={'limit': 100, 'fromId': last_id})
                                    for item in fills: 
                                        item['info'] = {**item}
                                        item['takerOrMaker'] = "maker" if item['maker'] else "taker"
                                    fillsValue[symbol] = Mapping().mapping_fills(
                                        exchange=exchange,
                                        fills=fills,
                                    )
                                else:
                                    fillsValue[symbol] = Helper().get_fills(exch=exch, symbol=symbol, limit=100, params={'fromId': last_id})
                                # fillsValue[symbol] = Mapping().mapping_fills(
                                #     exchange=exchange,
                                #     fills=Helper().get_fills(
                                #         exch=exch,
                                #         params={
                                #             "symbol": symbol,
                                #             "limit": 100,
                                #             "fromId": last_id,
                                #         },
                                #     ),
                                # )
            
                except ccxt.ExchangeError as e:
                    logger.warning(client + " " + exchange + " " + sub_account + " fills " + str(e))
                    pass
               
        fills = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass
        
        for symbol in fillsValue:
            for item in fillsValue[symbol]:
                new_value = {
                    "client": client,
                    "venue": exchange,
                    "account": "Main Account",
                    "fills_value": item,
                    "symbol": symbol,
                    "active": True,
                    "entry": False,
                    "exit": False,
                    "timestamp": datetime.now(timezone.utc),
                }

                if sub_account:
                    new_value["account"] = sub_account
                if spot:
                    new_value["spotMarket"] = spot
                if future:
                    new_value["futureMarket"] = future
                if perp:
                    new_value["perpMarket"] = perp

                new_value["runid"] = latest_run_id

                fills.append(new_value)

        del fillsValue

        if len(fills) <= 0:
            return True

        try:
            self.fills_db.insert_many(fills)

            del fills

            return True

        except Exception as e:
            logger.error(client + " " + exchange + " " + sub_account + " fills " + str(e))
            return True
