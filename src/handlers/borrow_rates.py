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

class BorrowRates:
    def __init__(self, db):
        if config['mode'] == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.borrow_rates_db = MongoDB(config['mongo_db'], db)
        else:
            self.borrow_rates_db = database_connector(db)
            self.runs_db = database_connector('runs')

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
            results = self.borrow_rates_db.aggregate(pipeline)
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
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()

            query = {}
            if client:
                query["client"] = client
            if exchange:
                query["venue"] = exchange
            if sub_account:
                query["account"] = sub_account

            borrow_rate_values = self.borrow_rates_db.find(query)

            current_values = None
            for item in borrow_rate_values:
                current_values = item['borrow_rates_value']

            borrow_rates_value = {}
            if current_values is None:
                for code in config['borrow_rates']['codes']:
                    if exchange == 'okx':
                        borrow_rates_value[code] = OKXHelper().get_borrow_rates(exch = exch, limit=92, code=code)
                    else:
                        borrow_rates_value[code] = Helper().get_borrow_rates(exch = exch, limit=92, code=code)
            else:
                for code in config['borrow_rates']['codes']:
                    last_time = int(current_values[code][-1]['timestamp'])
                    if exchange == 'okx':
                        borrow_rates_value[code] = current_values[code] + OKXHelper().get_borrow_rates(exch = exch, limit=92, code=code, since=last_time)
                    else:
                        borrow_rates_value[code] = current_values[code] + Helper().get_borrow_rates(exch = exch, limit=92, code=code, since=last_time)

        borrow_rates = {
            "client": client,
            "venue": exchange,
            "account": "Main Account",
            "borrow_rates_value": borrow_rates_value,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            borrow_rates["account"] = sub_account
        if spot:
            borrow_rates["spotMarket"] = spot
        if future:
            borrow_rates["futureMarket"] = future
        if perp:
            borrow_rates["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass
        
        borrow_rates["runid"] = latest_run_id
        
        try:
            self.borrow_rates_db.update_one(
                {
                    "client": borrow_rates["client"],
                    "venue": borrow_rates["venue"],
                    "account": borrow_rates["account"]
                },
                { "$set": {
                    "borrow_rates_value": borrow_rates["borrow_rates_value"],
                    "active": borrow_rates["active"],
                    "entry": borrow_rates["entry"],
                    "exit": borrow_rates["exit"],
                    "timestamp": borrow_rates["timestamp"],
                    "runid": borrow_rates["runid"]
                }},
                upsert=True
            )
                
            return borrow_rates
        except Exception as e:
            log.error(e)
            return False