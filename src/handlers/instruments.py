from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper

config = read_config_file()

class Instruments:
    def __init__(self, db, collection):

        self.runs_db = db['runs']
        self.instruments_db = db['instruments']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     position_type: str = None,
    #     exchange: str = None,
    #     account: str = None,
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
    #     if position_type:
    #         pipeline.append({"$match": {"positionType": position_type}})
    #     if exchange:
    #         pipeline.append({"$match": {"venue": exchange}})

    #     try:
    #         results = self.instruments_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

    def create(
        self,
        client: str = None,
        exch = None,
        exchange: str = None,
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        instrumentValue: str = None,
        bid_ask_value: str = None,
        logger=None
    ):
        if exch == None:
            exch = Exchange(exchange).exch()
            
        if instrumentValue is None:
            
            try:
                if exchange == "okx":
                    instrumentValue = OKXHelper().get_instruments(exch=exch)
                elif exchange == "binance":
                    instrumentValue = Helper().get_instruments(exch=exch)
                elif exchange == "bybit":
                    instrumentValue = BybitHelper().get_instruments(exch=exch)
            
            except ccxt.ExchangeError as e:
                if logger == None:
                    print(exchange +" instruments " + str(e))
                    print("Unable to collect instruments for " + exchange)
                else:
                    logger.warning(exchange +" instruments " + str(e))
                    logger.error("Unable to collect instruments for " + exchange)

                return True

        instrument = {
            "venue": exchange,
            "instrument_value": instrumentValue,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }
        del instrumentValue

        if spot:
            instrument["spotMarket"] = spot
        if future:
            instrument["futureMarket"] = future
        if perp:
            instrument["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass
        instrument["runid"] = latest_run_id
        
        try:
            if config["instruments"]["store_type"] == "snapshot":
                self.instruments_db.update_one(
                    {
                        "venue": instrument["venue"],
                    },
                    { "$set": {
                        "instrument_value": instrument["instrument_value"],
                        "timestamp": instrument["timestamp"],
                        "runid": instrument["runid"]
                    }},
                    upsert=True
                )
                
                
            elif config["instruments"]["store_type"] == "timeseries":
                self.instruments_db.insert_one(instrument)

            del instrument

            if logger == None:
                print("Collected instruments for " + exchange)
            else:
                logger.info("Collected instruments for " + exchange)

            return True

        except Exception as e:
            if logger == None:
                print(exchange +" instruments " + str(e))
                print("Unable to collect instruments for " + exchange)
            else:
                logger.error(exchange +" instruments " + str(e))
                logger.error("Unable to collect instruments for " + exchange)

            return True

