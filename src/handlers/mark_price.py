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
# from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class MarkPrices:
    def __init__(self, db, collection):
        # if os.getenv("mode") == "testing":
        #     self.runs_db = MongoDB(config["mongo_db"], "runs")
        #     self.positions_db = MongoDB(config["mongo_db"], "positions")
        #     self.mark_prices_db = MongoDB(config["mongo_db"], db)
        # else:
        #     self.runs_db = database_connector("runs")
        #     self.positions_db = database_connector("positions")
        #     self.mark_prices_db = database_connector(db)

        self.runs_db = db['runs']
        self.positions_db = db['positions']
        self.mark_prices_db = db['mark_prices']

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
            self.positions_db.close()
            self.mark_prices_db.close()
        else:
            self.runs_db.database.client.close()
            self.positions_db.database.client.close()
            self.mark_prices_db.database.client.close()

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
        account: str = None,
        symbol: str = None,
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
        if symbol:
            pipeline.append({"$match": {"symbol": symbol}})

        try:
            results = self.mark_prices_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        symbol: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        markPriceValue: str = None,
        back_off = {},
        logger=None
    ):
        if markPriceValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()
            
            markPriceValue = None
            try:
                if exchange == "okx":
                    markPriceValue = OKXHelper().get_mark_prices(
                        exch=exch, symbol=symbol+"-USDT-SWAP"
                    )
                elif exchange == "binance":
                    markPriceValue = Helper().get_mark_prices(
                        exch=exch, symbol=symbol+"USDT"
                    )
                elif exchange == "bybit":
                    markPriceValue = BybitHelper().get_mark_prices(
                        exch=exch, symbol=symbol+"USDT"
                    )

            # except ccxt.InvalidNonce as e:
            #     print("Hit rate limit", e)
            #     time.sleep(back_off[exchange] / 1000.0)
            #     back_off[exchange] *= 2
            #     return True
            
            except ccxt.ExchangeError as e:
                logger.warning(exchange +" mark prices " + str(e))
                # print("An error occurred in Mark Prices:", e)
                return True
            except ccxt.NetworkError as e:
                logger.warning(exchange +" mark prices " + str(e))
                return False

        # back_off[exchange] = config['dask']['back_off']
        
        mark_price = {
            "venue": exchange,
            "mark_price_value": markPriceValue,
            "symbol": symbol+"/USDT",
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if spot:
            mark_price["spotMarket"] = spot
        if future:
            mark_price["futureMarket"] = future
        if perp:
            mark_price["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass
        mark_price["runid"] = latest_run_id

        # get latest mark_prices data
        query = {}
        if exchange:
            query["venue"] = exchange
            query['symbol'] = symbol+"/USDT"

        mark_price_values = self.mark_prices_db.find(query).sort("runid", -1).limit(1)

        latest_run_id = -1
        latest_value = None
        for item in mark_price_values:
            if latest_run_id < item["runid"]:
                latest_run_id = item["runid"]
                latest_value = item["mark_price_value"]

        if latest_value == mark_price["mark_price_value"]:
            print("same mark price")
            return True

        try:
            if config["mark_prices"]["store_type"] == "timeseries":
                self.mark_prices_db.insert_one(mark_price)
            elif config["mark_prices"]["store_type"] == "snapshot":
                self.mark_prices_db.update_one(
                    {
                        "venue": mark_price["venue"],
                        "symbol": mark_price['symbol']
                    },
                    {
                        "$set": {
                            "mark_price_value": mark_price["mark_price_value"],
                            "active": mark_price["active"],
                            "entry": mark_price["entry"],
                            "exit": mark_price["exit"],
                            "timestamp": mark_price["timestamp"],
                            "runid": mark_price["runid"],
                        }
                    },
                    upsert=True,
                )

            del mark_price

            return True

            # return mark_price
        except Exception as e:
            logger.error(exchange +" mark prices " + str(e))
            return True
