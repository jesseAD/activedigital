import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.config import read_config_file
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()

class Leverages:
    def __init__(self, db):
        self.leverages_db = MongoDB(config['mongo_db'], db)
        self.positions_db = MongoDB(config['mongo_db'], 'positions')
        self.balances_db = MongoDB(config['mongo_db'], 'balances')
        self.tickers_db = MongoDB(config['mongo_db'], 'tickers')
        self.runs_db = MongoDB(config['mongo_db'], 'runs')
        self.runs_cloud = database_connector('runs')
        self.leverages_db = MongoDB(config['mongo_db'], 'leverages')
        self.leverages_cloud = database_connector('leverages')

    def get(
        self,
        client,
        exchange: str = None,
        account: str = None,
    ):
        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass

        try:
            query = {}
            if client:
                query["client"] = client
            if exchange:
                query["venue"] = exchange
            if account:
                query["account"] = account
            query["runid"] = latest_run_id
            
            # fetch latest position, balance, tickers
            position_value = self.positions_db.find(query).sort('_id', -1).limit(1)
            for item in position_value:
                latest_position = item['position_value']
            print('------', latest_position)
            balance_valule = self.balances_db.find(query).sort('_id', -1).limit(1)
            for item in balance_valule:
                latest_balance = item['balance_value']
            ticker_value = self.tickers_db.find(query).sort('_id', -1).limit(1)
            for item in ticker_value:
                latest_ticker = item['ticker_value']['last']

            max_notional = abs(float(max(latest_position, key=lambda x: abs(float(x['notional'])))['notional']))
            
            base_currency = config['balances']['base_ccy']

            balance_in_base_currency = 0
            for currency, balance in latest_balance.items():
                if currency == base_currency:
                    balance_in_base_currency += balance
                else:
                    balance_in_base_currency += latest_ticker * balance

            leverage_value = {
                "client": client,
                "venue": exchange,
                "account": account,
                "timestamp": datetime.now(timezone.utc),
            }
            leverage_value['leverage'] = max_notional / balance_in_base_currency
            leverage_value["runid"] = latest_run_id
            self.leverages_db.insert(leverage_value)
            self.leverages_cloud.insert_one(leverage_value)
            self.runs_db.update({"runid": latest_run_id}, {"end_time": datetime.now(timezone.utc)})
            self.runs_cloud.update_one({"runid": latest_run_id}, {"$set": {"end_time": datetime.now(timezone.utc)}})

            return leverage_value
        
        except Exception as e:
            log.error(e)