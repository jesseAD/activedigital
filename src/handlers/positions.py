import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()

class Positions:
    def __init__(self, db):
        if config['mode'] == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.positions_db = MongoDB(config['mongo_db'], db)
        else:
            self.runs_db = database_connector('runs')
            self.positions_db = database_connector('positions')

    def get(
        self,
        client,
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
        if client:
            pipeline.append({"$match": {"client": client}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})
        if account:
            pipeline.append({"$match": {"account": account}})

        try:
            results = self.positions_db.aggregate(pipeline)
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
        position_value: str = None
    ):
        if position_value is None:
            spec = client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            PASSPHRASE = None
            if exchange == "okx":
                PASSPHRASE = os.getenv(spec + "PASSPHRASE")

            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE).exch()
            # print(exch.papi_get_balance ())
            
            if exchange == 'okx':
                position_value = OKXHelper().get_positions(exch = exch)
            else:
                position_value = Helper().get_positions(exch = exch)
        
        position_info =[]
        for value in position_value:
                if float(value['initialMargin']) > 0:
                    portfolio = None
                    if exchange == 'binance':
                        if config['positions']['margin_mode'] == "non_portfolio":
                            portfolio = Helper().get_non_portfolio_margin(exch=exch, params={'symbol': value['info']['symbol']})
                        elif config['positions']['margin_mode'] == "portfolio":
                            portfolio = Helper().get_portfolio_margin(exch=exch, params={'symbol': 'USDT'})
                            portfolio = [item for item in portfolio if float(item['balance']) != 0]
                    value['margin'] = portfolio
                    position_info.append(value)

        current_time = datetime.now(timezone.utc)
        position = {
            "client": client,
            "venue": exchange,
            # "positionType": positionType.lower(),
            "account": "Main Account",
            "position_value": Mapping().mapping_positions(exchange=exchange, positions=position_info),
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": current_time,
        }

        if sub_account:
            position["account"] = sub_account
        if spot:
            position["spotMarket"] = spot
        if future:
            position["futureMarket"] = future
        if perp:
            position["perpMarket"] = perp

        # get latest positions data
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account

        position_values = self.positions_db.find(query).sort('_id', -1)

        latest_run_id = -1
        latest_value = None
        for item in position_values:
            if latest_run_id < item['runid']:
                latest_run_id = item['runid']
                latest_value = item['position_value']
        
        if latest_value == position['position_value']:
            print('same position')
            return False

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid'] + 1
            except:
                pass
        
        try:
            self.runs_db.insert_one({"start_time": current_time, "runid": latest_run_id})
            position["runid"] = latest_run_id

            if config['positions']['store_type'] == "timeseries": 
                self.positions_db.insert_one(position)   
            elif config['positions']['store_type'] == "snapshot": 
                self.positions_db.update_one(
                    {
                        "client": position["client"],
                        "venue": position["venue"],
                        "account": position["account"]
                    },
                    { "$set": {
                        "position_value": position["position_value"],
                        "active": position["active"],
                        "entry": position["entry"],
                        "exit": position["exit"],
                        "timestamp": position["timestamp"],
                        "runid": position["runid"]
                    }},
                    upsert=True
                )
                
            # log.debug(f"Position created: {position}")
            return position
        except Exception as e:
            log.error(e)
            return False

    def entry(self, account: str = None, status: bool = True):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            try:
                self.positions_db.update(
                    {"_id": position["_id"]},
                    {"$set": {"entry": status}},
                )
                log.debug(
                    f"position in account entry {account} has been set to {status}"
                )
            except Exception as e:
                log.error(e)
                return False

        return True

    def exit(self, account: str = None, status: bool = False):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            if position["entry"] is False:
                log.debug(
                    f"position in account {account} has not been entered, skipping"
                )
                continue
            try:
                self.positions_db.update(
                    {"_id": position["_id"]},
                    {"$set": {"exit": status}},
                )
                log.debug(
                    f"Position in account exit {account} has been set to {status}"
                )
            except Exception as e:
                log.error(e)
                return False

        return True

    def update(self, account: str = None, **kwargs: dict):
        # get all positions with account
        positions = Positions.get(account=account)

        for position in positions:
            try:
                self.positions_db.update_one(
                    {"_id": position["_id"]},
                    {"$set": kwargs},
                )
                log.debug(f"Position in account {account} has been updated")
            except Exception as e:
                log.error(e)
                return False

        return True
