from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper

config = read_config_file()


class MarkPrices:
    def __init__(self, db, collection):

        self.runs_db = db['runs']
        self.positions_db = db['positions']
        self.mark_prices_db = db['mark_prices']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     exchange: str = None,
    #     account: str = None,
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
    #     if exchange:
    #         pipeline.append({"$match": {"venue": exchange}})
    #     if account:
    #         pipeline.append({"$match": {"account": account}})
    #     if symbol:
    #         pipeline.append({"$match": {"symbol": symbol}})

    #     try:
    #         results = self.mark_prices_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        symbols: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        markPriceValue: str = None,
        logger=None
    ):
        if markPriceValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()
            
            markPriceValue = {}
            try:
                if exchange == "okx":
                    res = OKXHelper().get_mark_prices(exch=exch)
                    symbols_set = set(symbols)
                    markPriceValue = {item['symbol'][:-10]: item for item in res if item['symbol'][:-10] in symbols_set}
                                
                elif exchange == "binance":
                    res = Helper().get_mark_prices(exch=exch)
                    symbols_set = set(symbols)
                    markPriceValue = {item['symbol'][:-4]: item for item in res if item['symbol'][:-4] in symbols_set}

                elif exchange == "bybit":
                    res = BybitHelper().get_mark_prices(exch=exch, symbol=symbols+"USDT")
                    markPriceValue = {symbols: res}
            
            except ccxt.ExchangeError as e:
                logger.warning(exchange +" mark prices " + str(e))
                return True
            except ccxt.NetworkError as e:
                logger.warning(exchange +" mark prices " + str(e))
                return False

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        mark_prices = []
        for _key, _val in markPriceValue.items():
            mark_price = {
                "venue": exchange,
                "mark_price_value": _val,
                "symbol": _key+"/USDT",
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": datetime.now(timezone.utc),
            }

            if spot:
                mark_price["spotMarket"] = spot
            if future:
                mark_price["futureMarket"] = future
            if perp:
                mark_price["perpMarket"] = perp

            mark_price["runid"] = latest_run_id

            mark_prices.append(mark_price)

        try:
            if config["mark_prices"]["store_type"] == "timeseries":
                self.mark_prices_db.insert_many(mark_prices)
            elif config["mark_prices"]["store_type"] == "snapshot":
                self.mark_prices_db.update_one(
                    {
                        "venue": mark_price["venue"],
                        "symbol": mark_price['symbol']
                    },
                    {
                        "$set": {
                            "mark_price_value": mark_price["mark_price_value"],
                            "active": mark_price["active"],
                            "entry": mark_price["entry"],
                            "exit": mark_price["exit"],
                            "timestamp": mark_price["timestamp"],
                            "runid": mark_price["runid"],
                        }
                    },
                    upsert=True,
                )

            del mark_price

            return True

        except Exception as e:
            logger.error(exchange +" mark prices " + str(e))
            return True
