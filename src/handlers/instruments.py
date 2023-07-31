import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import gzip
import pickle

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
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
        self.runs_db = MongoDB(config["mongo_db"], "runs")
        self.insturments_db = MongoDB(config["mongo_db"], db)
        self.instruments_cloud = database_connector("instruments")

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
        if account:
            pipeline.append({"$match": {"account": account}})

        try:
            results = self.insturments_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client,
        exchange: str = None,
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        instrumentValue: str = None,
    ):
        if instrumentValue is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()
            if exchange == "okx":
                instrumentValue = OKXHelper().get_instruments(exch=exch)
            else:
                instrumentValue = Helper().get_instruments(exch=exch)

        instrument = {
            "client": client,
            "venue": exchange,
            # "positionType": positionType.lower(),
            "account": "Main Account",
            "instrument_value": instrumentValue[0]["info"],
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            instrument["account"] = sub_account
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

        instrument_values = self.insturments_db.find(query).sort("_id", -1).limit(1)

        try:
            if config["instruments"]["store_type"] == "snapshot":
                self.insturments_db.update(
                    {
                        "client": instrument["client"],
                        "venue": instrument["venue"],
                        "account": instrument["account"]
                    },
                    {
                        "instrument_value": instrument["instrument_value"],
                        "timestamp": instrument["timestamp"],
                        "runid": instrument["runid"]
                    },
                    upsert=True
                )
                
                self.instruments_cloud.update_one(
                    filter={
                        "client": instrument["client"],
                        "venue": instrument["venue"],
                        "account": instrument["account"]
                    },
                    update={"$set": {
                            "instrument_value": instrument["instrument_value"],
                            "timestamp": instrument["timestamp"],
                            "runid": instrument["runid"]
                        }
                    },
                    upsert = True
                )

                
            elif config["instruments"]["store_type"] == "timeseries":
                self.insturments_db.insert(instrument)
                self.instruments_cloud.insert_one(instrument)
            return instrument
        except Exception as e:
            log.error(e)
            return False

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
