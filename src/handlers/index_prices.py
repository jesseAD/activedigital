from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

config = read_config_file()
class IndexPrices:
  def __init__(self, db, collection):

    self.runs_db = db[config['mongodb']['database']]['runs']
    self.index_prices_db = db[config['mongodb']['database']]['index_prices']

  # def get(
  #   self,
  #   active: bool = None,
  #   spot: str = None,
  #   future: str = None,
  #   perp: str = None,
  #   exchange: str = None,
  # ):
  #   results = []

  #   pipeline = [
  #     {"$sort": {"_id": -1}},
  #   ]

  #   if active is not None:
  #     pipeline.append({"$match": {"active": active}})
  #   if spot:
  #     pipeline.append({"$match": {"spotMarket": spot}})
  #   if future:
  #     pipeline.append({"$match": {"futureMarket": future}})
  #   if perp:
  #     pipeline.append({"$match": {"perpMarket": perp}})
  #   if exchange:
  #     pipeline.append({"$match": {"venue": exchange}})

  #   try:
  #     results = self.index_prices_db.aggregate(pipeline)
  #     return results

  #   except Exception as e:
  #     log.error(e)

  def create(
    self,
    exch = None,
    exchange: str = None,
    symbols: str = None,
    spot: str = None,
    future: str = None,
    perp: str = None,
    indexPriceValue: str = None,
    logger=None
  ):
    if indexPriceValue is None:
      if exch == None:
        exch = Exchange(exchange).exch()

      indexPriceValue = {}

      try:
        if exchange == "okx":
          res = OKXHelper().get_index_prices(exch=exch)

          symbols_set = set(symbols)
          indexPriceValue = {item['symbol'][:-5]: item for item in res if item['symbol'][:-5] in symbols_set}

        elif exchange == "binance":
          res = Helper().get_index_prices(exch=exch)

          symbols_set = set(symbols)
          indexPriceValue = {item['symbol'][:-4]: item for item in res if item['symbol'][:-4] in symbols_set}

        elif exchange == "bybit":
          res = BybitHelper().get_index_prices(exch=exch, symbol=symbols+"USDT")

          indexPriceValue = {symbols: res}

        elif exchange == "huobi":
          res = HuobiHelper().get_index_prices(exch=exch, symbol=symbols+"-USDT")

          indexPriceValue = {symbols: res}
      
      except ccxt.ExchangeError as e:
        if logger == None:
          print(exchange +" index prices " + str(e))
          print("Unable to collect index prices for " + exchange)
        else:
          logger.warning(exchange +" index prices " + str(e))
          logger.error("Unable to collect index prices for " + exchange)

        return True
      
      except ccxt.NetworkError as e:
        if logger == None:
          print(exchange +" index prices " + str(e))
        else:
          logger.warning(exchange +" index prices " + str(e))

        return False
      
      except Exception as e:
        if logger == None:
          print(exchange +" index prices " + str(e))
          print("Unable to collect index prices for " + exchange)
        else:
          logger.warning(exchange +" index prices " + str(e))
          logger.error("Unable to collect index prices for " + exchange)

        return True
    
    run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item['runid']
      except:
        pass

    index_prices = []
    for _key, _val in indexPriceValue.items():
      index_price = {
        "venue": exchange,
        "index_price_value": _val,
        "symbol": _key+"/USDT",
        "active": True,
        "entry": False,
        "exit": False,
        "timestamp": datetime.now(timezone.utc),
      }

      if spot:
        index_price["spotMarket"] = spot
      if future:
        index_price["futureMarket"] = future
      if perp:
        index_price["perpMarket"] = perp

      index_price["runid"] = latest_run_id

      index_prices.append(index_price)
    
    if len(index_prices) <= 0:
      if logger == None:
        print(exchange +" empty index prices")
        print("Unable to collect index prices for " + exchange)
      else:
        logger.error(exchange +" empty index prices")
        logger.error("Unable to collect index prices for " + exchange)

      return True

    try:
      self.index_prices_db.insert_many(index_prices)

      del index_price

      if logger == None:
        print("Collected index prices for " + exchange)
      else:
        logger.info("Collected index prices for " + exchange)

      return True
        
    except Exception as e:
      if logger == None:
        print(exchange +" index prices " + str(e))
        print("Unable to collect index prices for " + exchange)
      else:
        logger.error(exchange +" index prices " + str(e))
        logger.error("Unable to collect index prices for " + exchange)

      return True
