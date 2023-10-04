import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 
import time

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
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config['mongo_db'], 'runs')
            self.tickers_db = MongoDB(config['mongo_db'], db)
        else:
            self.runs_db = database_connector('runs')
            self.tickers_db = database_connector('tickers')

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
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

        try:
            results = self.tickers_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client: str = None,
        exch = None,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        tickerValue: str = None,
        back_off = {},
    ):
        if tickerValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()
            
            try:
                if exchange == 'okx':
                    tickerValue = OKXHelper().get_tickers(exch = exch)
                else:
                    tickerValue = Helper().get_tickers(exch = exch)

            except ccxt.InvalidNonce as e:
                print("Hit rate limit", e)
                time.sleep(back_off[exchange] / 1000.0)
                back_off[exchange] *= 2
                return False
        
            except Exception as e:
                print("An error occurred in Tickers:", e)
                return False
        
        back_off[exchange] = config['dask']['back_off']
        
        tickerValue = {symbol: tickerValue[symbol] for symbol in config['tickers']['symbols'] if symbol in tickerValue}
        tickerValue['USDT/USD'] = CoinbaseHelper().get_usdt2usd_ticker(exch=Exchange(exchange='coinbase').exch())
        print(tickerValue)
        
        ticker = {
            "venue": exchange,
            "ticker_value": tickerValue,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        del tickerValue

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

        # get latest tickers data
        query = {}
        if exchange:
            query["venue"] = exchange

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
                        "venue": ticker["venue"]
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

            del ticker
                
            # return ticker
        except Exception as e:
            log.error(e)
            return False
