from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, CoinbaseHelper, HuobiHelper

config = read_config_file()

class Tickers:
    def __init__(self, db, collection):

        self.runs_db = db[config['mongodb']['database']]['runs']
        self.tickers_db = db[config['mongodb']['database']]['tickers']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     exchange: str = None,
    # ):
    #     results = []

    #     pipeline = [
    #         {"$sort": {"_id": -1}},
    #     ]

    #     if active is not None:
    #         pipeline.append({"$match": {"active": active}})
    #     if spot:
    #         pipeline.append({"$match": {"spotMarket": spot}})
    #     if future:
    #         pipeline.append({"$match": {"futureMarket": future}})
    #     if perp:
    #         pipeline.append({"$match": {"perpMarket": perp}})
    #     if exchange:
    #         pipeline.append({"$match": {"venue": exchange}})

    #     try:
    #         results = self.tickers_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

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
        logger=None
    ):
        if tickerValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()
            
            try:
                if exchange == 'okx':
                    tickerValue = OKXHelper().get_tickers(exch = exch)
                elif exchange == "binance":
                    tickerValue = Helper().get_tickers(exch = exch)
                elif exchange == "bybit":
                    tickerValue = BybitHelper().get_tickers(exch=exch)
                    for item in tickerValue:
                        tickerValue[item]['symbol'] = tickerValue[item]['symbol'].split(":")[0]

                    tickerValue = {_val['symbol']: _val for _key, _val in tickerValue.items()}
                elif exchange == "huobi":
                    tickerValue = HuobiHelper().get_tickers(exch = exch)
                
                tickerValue = {symbol: tickerValue[symbol] for symbol in tickerValue if symbol.endswith("USDT")}
                tickerValue['USDT/USD'] = CoinbaseHelper().get_usdt2usd_ticker(exch=Exchange(exchange='coinbase').exch())
        
            except ccxt.ExchangeError as e:
                if logger == None:
                    print(exchange +" tickers " + str(e))
                    print("Unable to collect tickers for " + exchange)
                else:
                    logger.warning(exchange +" tickers " + str(e))
                    logger.error("Unable to collect tickers for " + exchange)

                return True
        
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

            if logger == None:
                print("Collected tickers for " + exchange)
            else:
                logger.info("Collected tickers for " + exchange)

            return True
                
        except Exception as e:
            if logger == None:
                print(exchange +" tickers " + str(e))
                print("Unable to collect tickers for " + exchange)
            else:
                logger.error(exchange +" tickers " + str(e))
                logger.error("Unable to collect tickers for " + exchange)

            return True
