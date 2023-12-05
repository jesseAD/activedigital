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


class Instruments:
    def __init__(self, db):
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.bid_asks_db = MongoDB(config["mongo_db"], "bid_asks")
            self.insturments_db = MongoDB(config["mongo_db"], db)
        else:
            self.runs_db = database_connector("runs")
            self.bid_asks_db = database_connector("bid_asks")
            self.insturments_db = database_connector(db)

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
            self.bid_asks_db.close()
            self.insturments_db.close()
        else:
            self.runs_db.database.client.close()
            self.bid_asks_db.database.client.close()
            self.insturments_db.database.client.close()

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
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
        if position_type:
            pipeline.append({"$match": {"positionType": position_type}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})

        try:
            results = self.insturments_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client: str = None,
        exch = None,
        exchange: str = None,
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        instrumentValue: str = None,
        bid_ask_value: str = None,
        back_off = {},
        logger=None
    ):
        if exch == None:
            exch = Exchange(exchange).exch()
            
        if instrumentValue is None:
            
            try:
                if exchange == "okx":
                    instrumentValue = OKXHelper().get_instruments(exch=exch)
                elif exchange == "binance":
                    instrumentValue = Helper().get_instruments(exch=exch)
                elif exchange == "bybit":
                    instrumentValue = BybitHelper().get_instruments(exch=exch)

            # except ccxt.InvalidNonce as e:
            #     print("Hit rate limit", e)
            #     time.sleep(back_off[exchange] / 1000.0)
            #     back_off[exchange] *= 2
            #     return True
            
            except ccxt.ExchangeError as e:
                logger.warning(exchange +" instruments " + str(e))
                # print("An error occurred in Instruments:", e)
                pass

        instrument = {
            "venue": exchange,
            "instrument_value": instrumentValue,#Mapping().mapping_instruments(exchange=exchange, instrument=instrumentValue),
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }
        del instrumentValue

        if spot:
            instrument["spotMarket"] = spot
        if future:
            instrument["futureMarket"] = future
        if perp:
            instrument["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass
        instrument["runid"] = latest_run_id

        # get latest instruments data
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account

        instrument_values = self.insturments_db.find(query).sort('runid', -1).limit(1)

        latest_run_id = -1
        latest_value = None
        for item in instrument_values:
            if latest_run_id < item['runid']:
                latest_run_id = item['runid']
                latest_value = item['instrument_value']
        
        if latest_value == instrument['instrument_value']:
            print('same instrument')
            return True
        
        try:
            if config["instruments"]["store_type"] == "snapshot":
                self.insturments_db.update_one(
                    {
                        "venue": instrument["venue"],
                    },
                    { "$set": {
                        "instrument_value": instrument["instrument_value"],
                        "timestamp": instrument["timestamp"],
                        "runid": instrument["runid"]
                    }},
                    upsert=True
                )
                
                
            elif config["instruments"]["store_type"] == "timeseries":
                self.insturments_db.insert_one(instrument)

            del instrument

            return True

            # return instrument
        except Exception as e:
            logger.error(exchange +" instruments " + str(e))
            return True

    # def entry(self, account: str = None, status: bool = True):
    #     # get all positions with account
    #     positions = Positions.get(active=True, account=account)

    #     for position in positions:
    #         try:
    #             self.positions_db.update(
    #                 {"_id": position["_id"]},
    #                 {"entry": status},
    #             )
    #             log.debug(
    #                 f"position in account entry {account} has been set to {status}"
    #             )
    #         except Exception as e:
    #             log.error(e)
    #             return False

    #     return True

    # def exit(self, account: str = None, status: bool = False):
    #     # get all positions with account
    #     positions = Positions.get(active=True, account=account)

    #     for position in positions:
    #         if position["entry"] is False:
    #             log.debug(
    #                 f"position in account {account} has not been entered, skipping"
    #             )
    #             continue
    #         try:
    #             self.positions_db.update(
    #                 {"_id": position["_id"]},
    #                 {"exit": status},
    #             )
    #             log.debug(
    #                 f"Position in account exit {account} has been set to {status}"
    #             )
    #         except Exception as e:
    #             log.error(e)
    #             return False

    #     return True

    # def update(self, account: str = None, **kwargs: dict):
    #     # get all positions with account
    #     positions = Positions.get(account=account)

    #     for position in positions:
    #         try:
    #             self.positions_db.update(
    #                 {"_id": position["_id"]},
    #                 kwargs,
    #             )
    #             log.debug(f"Position in account {account} has been updated")
    #         except Exception as e:
    #             log.error(e)
    #             return False

    #     return True
