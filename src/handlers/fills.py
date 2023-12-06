import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 
import time

# from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
from src.lib.mapping import Mapping
# from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class Fills:
    def __init__(self, db, collection):
        # if os.getenv("mode") == "testing":
        #     self.runs_db = MongoDB(config["mongo_db"], "runs")
        #     self.positions_db = MongoDB(config["mongo_db"], "positions")
        #     self.fills_db = MongoDB(config["mongo_db"], db)
        # else:
        #     self.fills_db = database_connector(db)
        #     self.runs_db = database_connector("runs")
        #     self.positions_db = database_connector("positions")

        self.runs_db = db['runs']
        self.positions_db = db['positions']
        self.fills_db = db['fills']

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
            self.positions_db.close()
            self.fills_db.close()
        else:
            self.fills_db.database.client.close()
            self.runs_db.database.client.close()
            self.positions_db.database.client.close()

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
        account: str = None,
    ):
        results = []

        pipeline = [
            {"$sort": {"_id": -1}},
        ]

        if active is not None:
            pipeline.append({"$match": {"active": active}})
        if spot:
            pipeline.append({"$match": {"spotMarket": spot}})
        if future:
            pipeline.append({"$match": {"futureMarket": future}})
        if perp:
            pipeline.append({"$match": {"perpMarket": perp}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})
        if account:
            pipeline.append({"$match": {"account": account}})

        try:
            results = self.fills_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

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

            position_values = self.positions_db.find(query).sort("_id", -1).limit(1)
            symbols = []
            for position in position_values:
                for item in position['position_value']:
                    symbols.append(item['info']['symbol'])
            
            # if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'portfolio':
            #     symbols = [item.replace("/", "") for item in config["fills"]["symbols"][exchange]]
            #     symbols += [item[: -1] for item in symbols]
            # else:
            #     symbols = config["fills"]["symbols"][exchange]

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

                fills_values = self.fills_db.find(query).sort("_id", -1).limit(1)

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
                            if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'portfolio':
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
                                if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'portfolio':
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
                                if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'portfolio':
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
                
                # except ccxt.InvalidNonce as e:
                #     print("Hit rate limit", e)
                #     time.sleep(back_off[client + "_" + exchange + "_" + sub_account] / 1000.0)
                #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
                #     return False
            
                except ccxt.ExchangeError as e:
                    logger.warning(client + " " + exchange + " " + sub_account + " fills " + str(e))
                    # print("An error occurred in Fills:", e)
                    pass
               
        # back_off[client + "_" + exchange + "_" + sub_account] = config['dask']['back_off']
        
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

            # return fills
        except Exception as e:
            logger.error(client + " " + exchange + " " + sub_account + " fills " + str(e))
            return True
