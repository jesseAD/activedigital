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

class FundingRates:
    def __init__(self, db):
        if config['mode'] == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.funding_rates_db = MongoDB(config['mongo_db'], db)
        else:
            self.funding_rates_db = database_connector(db)
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
            results = self.funding_rates_db.aggregate(pipeline)
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

            funding_rate_values = self.funding_rates_db.find(query)

            current_values = None
            for item in funding_rate_values:
                current_values = item['funding_rates_value']

            funding_rates_value = {}
            if current_values is None:
                for symbol in config['funding_rates']['symbols']:
                    if exchange == 'okx':
                        funding_rates_value[symbol] = OKXHelper().get_funding_rates(exch = exch, limit=100, symbol=symbol)
                    else:
                        funding_rates_value[symbol] = Helper().get_funding_rates(exch = exch, limit=100, symbol=symbol)
            else:
                for symbol in config['funding_rates']['symbols']:
                    last_time = int(current_values[symbol][-1]['timestamp'])
                    if exchange == 'okx':
                        funding_rates_value[symbol] = current_values[symbol] + OKXHelper().get_funding_rates(exch = exch, limit=100, symbol=symbol, since=last_time)
                    else:
                        funding_rates_value[symbol] = current_values[symbol] + Helper().get_funding_rates(exch = exch, limit=100, symbol=symbol, since=last_time)

        funding_rates = {
            "client": client,
            "venue": exchange,
            "account": "Main Account",
            "funding_rates_value": funding_rates_value,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            funding_rates["account"] = sub_account
        if spot:
            funding_rates["spotMarket"] = spot
        if future:
            funding_rates["futureMarket"] = future
        if perp:
            funding_rates["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass
        
        funding_rates["runid"] = latest_run_id
        
        try:
            self.funding_rates_db.update_one(
                {
                    "client": funding_rates["client"],
                    "venue": funding_rates["venue"],
                    "account": funding_rates["account"]
                },
                { "$set": {
                    "funding_rates_value": funding_rates["funding_rates_value"],
                    "active": funding_rates["active"],
                    "entry": funding_rates["entry"],
                    "exit": funding_rates["exit"],
                    "timestamp": funding_rates["timestamp"],
                    "runid": funding_rates["runid"]
                }},
                upsert=True
            )
                
            return funding_rates
        except Exception as e:
            log.error(e)
            return False