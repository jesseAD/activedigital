import os
from dotenv import load_dotenv
from datetime import datetime, timezone

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

class Balances:
    def __init__(self, db):
        self.runs_db = MongoDB(config['mongo_db'], 'runs')
        self.balances_db = MongoDB(config['mongo_db'], db)
        self.balances_cloud = database_connector('balances')

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
            results = self.balances_db.aggregate(pipeline)
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
        balanceValue: str = None
    ):
        if balanceValue is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()
            if exchange == 'okx':
                balanceValue = OKXHelper().get_balances(exch = exch)
            else:
                balanceValue = Helper().get_balances(exch = exch)
        
        balance = {
            "client": client,
            "venue": exchange,
            # "positionType": positionType.lower(),
            "account": "Main Account",
            "balance_value": balanceValue,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            balance["account"] = sub_account
        if spot:
            balance["spotMarket"] = spot
        if future:
            balance["futureMarket"] = future
        if perp:
            balance["perpMarket"] = perp
        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass
        
        balance["runid"] = latest_run_id

        try:
            balance["runid"] = latest_run_id
            if config["balances"]["store_type"] == "timeseries":
                self.balances_db.insert(balance)
                self.balances_cloud.insert_one(balance)
            elif config["balances"]["store_type"] == "snapshot":
                self.balances_db.update(
                    {
                        "client": balance["client"],
                        "venue": balance["venue"],
                        "account": balance["account"]
                    },
                    {
                        "balance_value": balance["balance_value"],
                        "active": balance["active"],
                        "entry": balance["entry"],
                        "exit": balance["exit"],
                        "timestamp": balance["timestamp"],
                        "runid": balance["runid"]
                    },
                    upsert=True
                )
                
                self.balances_cloud.update_one(
                    {
                        "client": balance["client"],
                        "venue": balance["venue"],
                        "account": balance["account"]
                    },
                    {"$set": {
                        "balance_value": balance["balance_value"],
                        "active": balance["active"],
                        "entry": balance["entry"],
                        "exit": balance["exit"],
                        "timestamp": balance["timestamp"],
                        "runid": balance["runid"]
                        }
                    },
                    upsert=True
                )
                
            return balance
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
