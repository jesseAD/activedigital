import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.helpers import CoinbaseHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()

class Tickers:
    def __init__(self, db):
        if config['mode'] == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.positions_db = MongoDB(config['mongo_db'], 'positions')
            self.balances_db = MongoDB(config['mongo_db'], 'balances')
            self.tickers_db = MongoDB(config['mongo_db'], db)
        else:
            self.runs_db = database_connector('runs')
            self.balances_db = database_connector('balances')
            self.positions_db = database_connector('positions')
            self.tickers_db = database_connector('tickers')

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
        exchange: str = None,
        account: str = None,
        symbol: str = None
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
        if symbol:
            pipeline.append({"$match": {"symbol": symbol}})

        try:
            results = self.tickers_db.aggregate(pipeline)
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
        tickerValue: str = None,
    ):
        if tickerValue is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()
            if exchange == 'okx':
                tickerValue = OKXHelper().get_tickers(exch = exch)
            else:
                tickerValue = Helper().get_tickers(exch = exch)

        tickerValue = {symbol: tickerValue[symbol] for symbol in config['tickers']['symbols'] if symbol in tickerValue}
        tickerValue['USDT/USD'] = CoinbaseHelper().get_usdt2usd_ticker(exch=Exchange(exchange='coinbase').exch())
        
        ticker = {
            "client": client,
            "venue": exchange,
            "account": "Main Account",
            "ticker_value": tickerValue,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            ticker["account"] = sub_account
        if spot:
            ticker["spotMarket"] = spot
        if future:
            ticker["futureMarket"] = future
        if perp:
            ticker["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass
        ticker["runid"] = latest_run_id

        # insert usd base balance into positions collection
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account
        balance_values = self.balances_db.find(query).sort('runid', -1).limit(1)

        btc_balance = usdt_balance = 0
        for item in balance_values:
            try:
                btc_balance = item['balance_value']['BTC']
            except:
                pass
            try:
                usdt_balance = item['balance_value']['USDT']
            except:
                pass

        usd_base_balance = (usdt_balance + btc_balance * tickerValue['BTC/USDT']['last']) * tickerValue['USDT/USD']['last']
        
        self.positions_db.update_one(
            {
                'client': client,
                'venue': exchange,
                'account': sub_account,
                'runid': latest_run_id
            },
            {"$set": {
                'USDBaseBalance': usd_base_balance
            }}
        )

        # get latest tickers data
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account

        ticker_values = self.tickers_db.find(query).sort('runid', -1).limit(1)

        latest_run_id = -1
        latest_value = None
        for item in ticker_values:
            if latest_run_id < item['runid']:
                latest_run_id = item['runid']
                latest_value = item['ticker_value']
        
        if latest_value == ticker['ticker_value']:
            print('same ticker')
            return False
        
        try:
            if config['tickers']['store_type'] == "timeseries":
                self.tickers_db.insert_one(ticker)
            elif config['tickers']['store_type'] == "snapshot":
                self.tickers_db.update_one(
                    {
                        "client": ticker["client"],
                        "venue": ticker["venue"],
                        "account": ticker["account"],
                        "symbol": ticker["symbol"]
                    },
                    {"$set": {
                        "ticker_value": ticker["ticker_value"],
                        "active": ticker["active"],
                        "entry": ticker["entry"],
                        "exit": ticker["exit"],
                        "timestamp": ticker["timestamp"],
                        "runid": ticker["runid"]
                    }},
                    upsert=True
                )
                
            return ticker
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
