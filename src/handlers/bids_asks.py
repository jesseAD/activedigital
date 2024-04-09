from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper

config = read_config_file()

class Bids_Asks:
    def __init__(self, db, collection):

        self.runs_db = db[config['mongodb']['database']]['runs']
        self.bid_asks_db = db[config['mongodb']['database']]['bid_asks']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     position_type: str = None,
    #     exchange: str = None,
    #     symbol: str = None,
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
    #     if symbol:
    #         pipeline.append({"$match": {"symbol": symbol}})

    #     try:
    #         results = self.bid_asks_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        symbol: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        bid_ask_value: str = None,
        logger=None
    ):
        if exch == None:
            exch = Exchange(exchange).exch()

        if bid_ask_value is None:
            bid_ask_value = {}

            try:
                if exchange == "okx":
                    spot_value = OKXHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")
                    perp_value = OKXHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT:USDT")

                    bid_ask_value = {
                        'spot': spot_value,
                        'perp': perp_value,
                        'spread': spot_value['mid_point'] - perp_value['mid_point'],
                    }

                elif exchange == "binance":
                    spot_value = Helper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")
                    perp_value = Helper().get_bid_ask(exch=exch, symbol=symbol+"/USDT:USDT")

                    bid_ask_value = {
                        'spot': spot_value,
                        'perp': perp_value,
                        'spread': spot_value['mid_point'] - perp_value['mid_point'],
                    }   
                
                elif exchange == "bybit":
                    spot_value = BybitHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")
                    perp_value = BybitHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT:USDT")

                    bid_ask_value = {
                        'spot': spot_value,
                        'perp': perp_value,
                        'spread': spot_value['mid_point'] - perp_value['mid_point'],
                    } 
        
            except ccxt.ExchangeError as e:
                if logger == None:
                    print(exchange +" bids and asks " + str(e))
                    print("Unable to collect bids and asks for " + exchange)
                else:
                    logger.warning(exchange +" bids and asks " + str(e))
                    logger.error("Unable to collect bids and asks for " + exchange)
                return True
            except ccxt.NetworkError as e:
                if logger == None:
                    print(exchange +" bids and asks " + str(e))
                else:
                    logger.warning(exchange +" bids and asks " + str(e))

                return False

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        # store best bid, ask, mid point

        new_value = {
            "venue": exchange,
            "bid_ask_value": bid_ask_value, 
            "symbol": symbol+"/USDT",
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
            "runid": latest_run_id,
        }
        if spot:
            new_value["spotMarket"] = spot
        if future:
            new_value["futureMarket"] = future
        if perp:
            new_value["perpMarket"] = perp

        try:
            self.bid_asks_db.insert_one(new_value)

            if logger == None:
                print("Collected bids and asks for " + exchange)
            else:
                logger.info("Collected bids and asks for " + exchange)

            return True
        
        except Exception as e:
            if logger == None:
                print(exchange +" bids and asks " + str(e))
                print("Unable to collect bids and asks for " + exchange)
            else:
                logger.error(exchange +" bids and asks " + str(e))
                logger.error("Unable to collect bids and asks for " + exchange)

            return True