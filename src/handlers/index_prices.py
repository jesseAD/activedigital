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

class IndexPrices:
    def __init__(self, db, collection):
        # if os.getenv("mode") == "testing":
        #     self.runs_db = MongoDB(config['mongo_db'], 'runs')
        #     self.index_prices_db = MongoDB(config['mongo_db'], db)
        # else:
        #     self.runs_db = database_connector('runs')
        #     self.index_prices_db = database_connector(db)

        self.runs_db = db['runs']
        self.index_prices_db = db['index_prices']

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
            self.index_prices_db.close()
        else:
            self.runs_db.database.client.close()
            self.index_prices_db.database.client.close()

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
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

        try:
            results = self.index_prices_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        symbols: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        indexPriceValue: str = None,
        back_off = {},
        logger=None
    ):
        if indexPriceValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()

            indexPriceValue = {}

            try:
                if exchange == "okx":
                    res = OKXHelper().get_index_prices(exch=exch)

                    for symbol in symbols:
                        for item in res:
                            if item['symbol'].startswith(symbol):
                                indexPriceValue[symbol] = item

                elif exchange == "binance":
                    res = Helper().get_index_prices(exch=exch)

                    for symbol in symbols:
                        for item in res:
                            if item['symbol'].startswith(symbol):
                                indexPriceValue[symbol] = item

                elif exchange == "bybit":
                    res = BybitHelper().get_index_prices(exch=exch, symbol=symbols+"USDT")

                    indexPriceValue = {symbols: res}

            # except ccxt.InvalidNonce as e:
            #     print("Hit rate limit", e)
            #     time.sleep(back_off[exchange] / 1000.0)
            #     back_off[exchange] *= 2
            #     return True
            
            except ccxt.ExchangeError as e:
                logger.warning(exchange +" index prices " + str(e))
                # print("An error occurred in Index Prices:", e)
                return True
            except ccxt.NetworkError as e:
                logger.warning(exchange +" index prices " + str(e))
                return False
        
        # back_off[exchange] = config['dask']['back_off']

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass

        index_prices = []
        for _key, _val in indexPriceValue.items():
            index_price = {
                "venue": exchange,
                "index_price_value": _val,
                "symbol": _key+"/USDT",
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": datetime.now(timezone.utc),
            }

            if spot:
                index_price["spotMarket"] = spot
            if future:
                index_price["futureMarket"] = future
            if perp:
                index_price["perpMarket"] = perp

            index_price["runid"] = latest_run_id

            index_prices.append(index_price)
        
        try:
            self.index_prices_db.insert_many(index_prices)

            del index_price

            return True
                
            # return index_price
        except Exception as e:
            logger.error(exchange +" index prices " + str(e))
            return True
