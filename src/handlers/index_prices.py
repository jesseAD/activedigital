import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 
import time

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.helpers import CoinbaseHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()

class IndexPrices:
    def __init__(self, db):
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.index_prices_db = MongoDB(config['mongo_db'], db)
        else:
            self.runs_db = database_connector('runs')
            self.index_prices_db = database_connector(db)

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
        client: str = None,
        exch = None,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        indexPriceValue: str = None,
        back_off = None,
    ):
        if indexPriceValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()

            symbols = config['index_prices']['symbols'][exchange]
            indexPriceValue = {}

            for symbol in symbols:
                try:
                    if exchange == "okx":
                        symbol = symbol.replace('/', '-')
                        index_price = OKXHelper().get_index_prices(exch=exch, params={'instId': symbol})['data'][0]
                        indexPriceValue[symbol.replace('-', '')] = {
                            'indexPrice': index_price['idxPx'],
                            'timestamp': index_price['ts'],
                            'symbol': index_price['instId']
                        }
                    elif exchange == "binance":
                        symbol = symbol.replace('/', '')
                        index_price = Helper().get_mark_prices(exch=exch, params={'symbol': symbol})
                        indexPriceValue[symbol] = {
                            'indexPrice': index_price['indexPrice'],
                            'timestamp': index_price['time'],
                            'symbol': index_price['symbol']
                        }

                except ccxt.InvalidNonce as e:
                    print("Hit rate limit", e)
                    time.sleep(back_off[exchange] / 1000.0)
                    back_off[exchange] *= 2
                    return False
                
                except Exception as e:
                    print("An error occurred in Index Prices:", e)
                    pass
        
        back_off[exchange] = config['back_off']

        index_price = {
            "venue": exchange,
            "index_price_value": indexPriceValue,
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

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass
        index_price["runid"] = latest_run_id
        
        try:
            self.index_prices_db.insert_one(index_price)
                
            return index_price
        except Exception as e:
            log.error(e)
            return False
