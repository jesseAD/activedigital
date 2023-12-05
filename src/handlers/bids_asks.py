import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import gzip
import pickle
import ccxt 
import time

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


def compress_list(data):
    serialized_data = pickle.dumps(data)
    compressed_data = gzip.compress(serialized_data)
    return compressed_data


class Bids_Asks:
    def __init__(self, db):
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.bid_asks_db = MongoDB(config["mongo_db"], db)
        else:
            self.runs_db = database_connector("runs")
            self.bid_asks_db = database_connector(db)

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
            self.bid_asks_db.close()
        else:
            self.runs_db.database.client.close()
            self.bid_asks_db.database.client.close()

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
        exchange: str = None,
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
        if position_type:
            pipeline.append({"$match": {"positionType": position_type}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})
        if symbol:
            pipeline.append({"$match": {"symbol": symbol}})

        try:
            results = self.bid_asks_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        bid_ask_value: str = None,
        back_off = {},
        logger=None
    ):
        if exch == None:
            exch = Exchange(exchange).exch()

        if bid_ask_value is None:
            bid_ask_value = {}

            for i in range(len(config['bid_ask']['spot'])):
                try:
                    if exchange == "okx":
                        spot_value = OKXHelper().get_bid_ask(exch=exch, symbol=config['bid_ask']['spot'][i])
                        perp_value = OKXHelper().get_bid_ask(exch=exch, symbol=config['bid_ask']['perp'][i])

                        bid_ask_value[config['bid_ask']['spot'][i]] = {
                            'spot': spot_value,
                            'perp': perp_value,
                            'spread': spot_value['mid_point'] - perp_value['mid_point'],
                        }

                    elif exchange == "binance":
                        spot_value = Helper().get_bid_ask(exch=exch, symbol=config['bid_ask']['spot'][i])
                        perp_value = Helper().get_bid_ask(exch=exch, symbol=config['bid_ask']['perp'][i])

                        bid_ask_value[config['bid_ask']['spot'][i]] = {
                            'spot': spot_value,
                            'perp': perp_value,
                            'spread': spot_value['mid_point'] - perp_value['mid_point'],
                        }   
                    
                    elif exchange == "bybit":
                        spot_value = Helper().get_bid_ask(exch=exch, symbol=config['bid_ask']['spot'][i])
                        perp_value = Helper().get_bid_ask(exch=exch, symbol=config['bid_ask']['perp'][i])

                        bid_ask_value[config['bid_ask']['spot'][i]] = {
                            'spot': spot_value,
                            'perp': perp_value,
                            'spread': spot_value['mid_point'] - perp_value['mid_point'],
                        } 

                # except ccxt.InvalidNonce as e:
                #     print("Hit rate limit", e)
                #     time.sleep(back_off[exchange] / 1000.0)
                #     back_off[exchange] *= 2
                #     return True
            
                except ccxt.ExchangeError as e:
                    logger.warning(exchange +" bids and asks " + str(e))
                    # print("An error occurred in Bids and Asks:", e)
                    pass

        # back_off[exchange] = config['dask']['back_off']

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        # store best bid, ask, mid point
        bid_ask = []

        for _key, _value in bid_ask_value.items():
            new_value = {
                "venue": exchange,
                "bid_ask_value": _value, 
                "symbol": _key,
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": datetime.now(timezone.utc),
                "runid": latest_run_id,
            }
            if spot:
                new_value["spotMarket"] = spot
            if future:
                new_value["futureMarket"] = future
            if perp:
                new_value["perpMarket"] = perp

            bid_ask.append(new_value)

        del bid_ask_value

        try:
            self.bid_asks_db.insert_many(bid_ask)
            del bid_ask

            return True
        
        except Exception as e:
            logger.error(exchange +" bids and asks " + str(e))
            return True