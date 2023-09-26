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
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.tickers_db = MongoDB(config['mongo_db'], 'tickers')
            self.balances_db = MongoDB(config['mongo_db'], db)
        else:
            self.balances_db = database_connector(db)
            self.tickers_db = database_connector('tickers')
            self.runs_db = database_connector('runs')

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
        client: str = None,
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
        if client:
            pipeline.append({"$match": {"client": client}})
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
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        balanceValue: str = None
    ):
        if balanceValue is None:
            spec = client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            PASSPHRASE = None
            if exchange == "okx":
                PASSPHRASE = os.getenv(spec + "PASSPHRASE")

            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE).exch()
            
            try:
                if exchange == 'okx':
                    balanceValue = OKXHelper().get_balances(exch = exch)
                else:
                    balanceValue = Helper().get_balances(exch = exch)
            except:
                return False

        query = {}
        if exchange:
            query["venue"] = exchange
        ticker_values = self.tickers_db.find(query).sort('_id', -1).limit(1)

        for item in ticker_values:
            ticker_value = item['ticker_value']

        usd_balance = 0
        for _key, _value in balanceValue.items():
            if _key == "USDT":
                usd_balance += _value * ticker_value['USDT/USD']['last']
            else :
                usd_balance += _value * ticker_value[_key + '/USDT']['last'] * ticker_value['USDT/USD']['last']

        balanceValue['USD'] = usd_balance
        
        balance = {
            "client": client,
            "venue": exchange,
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

        # get latest balances data
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account

        balances_values = self.balances_db.find(query).sort('runid', -1).limit(1)

        latest_run_id = -1
        latest_value = None
        for item in balances_values:
            if latest_run_id < item['runid']:
                latest_run_id = item['runid']
                latest_value = item['balance_value']
        
        if latest_value == balance['balance_value']:
            print('same balance')
            return False
        
        try:
            if config["balances"]["store_type"] == "timeseries":
                self.balances_db.insert_one(balance)
            elif config["balances"]["store_type"] == "snapshot":
                self.balances_db.update_one(
                    {
                        "client": balance["client"],
                        "venue": balance["venue"],
                        "account": balance["account"]
                    },
                    { "$set": {
                        "balance_value": balance["balance_value"],
                        "active": balance["active"],
                        "entry": balance["entry"],
                        "exit": balance["exit"],
                        "timestamp": balance["timestamp"],
                        "runid": balance["runid"]
                    }},
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
