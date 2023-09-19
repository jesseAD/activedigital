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
        if os.getenv("mode") == "testing":
            self.leverages_db = MongoDB(config['mongo_db'], db)
            self.positions_db = MongoDB(config['mongo_db'], 'positions')
            self.balances_db = MongoDB(config['mongo_db'], 'balances')
            self.tickers_db = MongoDB(config['mongo_db'], 'tickers')
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
        else: 
            self.leverages_db = database_connector('leverages')
            self.positions_db = database_connector('positions')
            self.balances_db = database_connector('balances')
            self.tickers_db = database_connector('tickers')
            self.runs_db = database_connector('runs')

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
            if exchange:
                query["venue"] = exchange
            
            # fetch latest position, balance, tickers
            ticker_value = self.tickers_db.find(query).sort('runid', -1).limit(1)
            for item in ticker_value:
                try: 
                    latest_ticker = item['ticker_value']
                except Exception as e:
                    print("An error occurred in Leverages:", e)
                    return False  
                
            if client:
                query["client"] = client
            if account:
                query["account"] = account

            position_value = self.positions_db.find(query).sort('runid', -1).limit(1)
            for item in position_value:
                try:
                    latest_position = item['position_value']
                except Exception as e:
                    print("An error occurred in Leverages:", e)
                    return False
            
            balance_valule = self.balances_db.find(query).sort('runid', -1).limit(1)
            for item in balance_valule:
                try: 
                    latest_balance = item['balance_value']
                except Exception as e:
                    print("An error occurred in Leverages:", e)
                    return False

            try:
                max_notional = abs(float(max(latest_position, key=lambda x: abs(float(x['notional'])))['notional']))
            except Exception as e:
                print("An error occurred in Leverages:", e)
                return False
            
            base_currency = config['balances']['base_ccy']

            balance_in_base_currency = 0
            try:
                if base_currency == "USDT":
                    balance_in_base_currency = latest_balance['USD'] / latest_ticker['USDT/USD']['last']
                elif base_currency == "USD":
                    balance_in_base_currency = latest_balance['USD'] 
                else:
                    balance_in_base_currency = latest_balance['USD'] / (latest_ticker['USDT/USD']['last'] * latest_ticker[base_currency + '/USDT']['last'])
            except Exception as e:
                print("An error occurred in Leverages:", e)
                return False

            # try:
            #     balance_in_base_currency = 0
            #     for currency, balance in latest_balance.items():
            #         if currency == base_currency:
            #             balance_in_base_currency += balance
            #         else:
            #             balance_in_base_currency += latest_ticker * balance
            # except Exception as e:
            #     print("An error occurred in Leverages:", e)
            #     return False

            leverage_value = {
                "client": client,
                "venue": exchange,
                "account": account,
                "timestamp": datetime.now(timezone.utc),
            }
            try:
                leverage_value['leverage'] = max_notional / balance_in_base_currency
            except Exception as e:
                print("An error occurred in Leverages:", e)
                return False
            
            leverage_value["runid"] = latest_run_id
            
            if config['leverages']['store_type'] == "timeseries":
                self.leverages_db.insert_one(leverage_value)
            elif config['leverages']['store_type'] == "snapshot":
                self.leverages_db.update_one(
                    {
                        "client": leverage_value["client"],
                        "venue": leverage_value["venue"],
                        "account": leverage_value["account"]
                    },
                    { "$set": {
                        "leverage": leverage_value["leverage"],
                        "timestamp": leverage_value["timestamp"],
                        "runid": leverage_value["runid"]
                    }},
                    upsert=True
                )
                
            self.runs_db.update_one({"runid": latest_run_id}, {"$set": {"end_time": datetime.now(timezone.utc)}})

            return leverage_value
        
        except Exception as e:
            log.error(e)