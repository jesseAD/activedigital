import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.config import read_config_file
from src.lib.exchange import Exchange
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.positions import Positions
from src.handlers.balances import Balances
from src.handlers.tickers import Tickers
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()

class Levarages:
    def __init__(self, db):
        self.leverages_db = MongoDB(config['mongo_db'], db)
        self.positions_db = MongoDB(config['mongo_db'], 'positions')
        self.balances_db = MongoDB(config['mongo_db'], 'balances')
        self.leverages_db = MongoDB(config['mongo_db'], 'leverages')
        self.leverages_cloud = database_connector('leverages')

    def get(
        self,
        client,
        exchange: str = None,
        account: str = None,
    ):        
        spec = exchange.upper() + "_" + account.upper() + "_"
        API_KEY = os.getenv(spec + "API_KEY")
        API_SECRET = os.getenv(spec + "API_SECRET")
        exch = Exchange(exchange, account, API_KEY, API_SECRET).exch()
        
        if exchange == 'okx':
            helper = OKXHelper()
        else:
            helper = Helper()

        try:
            position_value = helper.get_positions(exch)
            position_info = []
            for value in position_value:
                if float(value['initialMargin']) > 0:
                    position_info.append(value)

            max_notional = abs(float(max(position_info, key=lambda x: abs(float(x['notional'])))['notional']))
            
            balance_valule = helper.get_balances(exch)
            base_currency = config['balances']['base_ccy']

            balance_in_base_currency = 0
            for currency, balance in balance_valule.items():
                if currency == base_currency:
                    balance_in_base_currency += balance
                else:
                    ticker_value = helper.get_tickers(exch=exch, symbol=currency+'/'+base_currency)['last']
                    balance_in_base_currency += ticker_value * balance

            leverage_value = {
                "client": client,
                "venue": exchange,
                "account": account,
                "timestamp": datetime.now(timezone.utc),
            }
            leverage_value['leverage'] = max_notional / balance_in_base_currency
            self.leverages_db.insert(leverage_value)
            self.leverages_cloud.insert_one(leverage_value)
            
            return leverage_value
        
        except Exception as e:
            log.error(e)