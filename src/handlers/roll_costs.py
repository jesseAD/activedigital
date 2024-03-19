from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.lib.expiry_date import get_expiry_date
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
from src.config import read_config_file

config = read_config_file()

class Roll_Costs:
  def __init__(self, db, collection):

    self.runs_db = db['runs']
    self.roll_costs_db = db['roll_costs']

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
    roll_cost_value: str = None,
    logger=None
  ):
    if exch == None:
      exch = Exchange(exchange).exch()

    if roll_cost_value is None:
      roll_cost_value = []

      for symbol in config['roll_costs']['symbols']:
        for prompt in config['roll_costs']['prompts']:
          expiry_date = get_expiry_date(prompt, datetime.now(timezone.utc))
          
          try:
            if exchange == "okx":
              expiry_str = expiry_date.strftime('%y%m%d')
              print(exchange + " " + symbol + " " + expiry_str)

              linear_ask = OKXHelper().get_future_ask(exch=exch, symbol=symbol+"-USDT-"+expiry_str)
              spot_bid = OKXHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")['bid']
              inverse_ask = OKXHelper().get_future_ask(exch=exch, symbol=symbol+"-USD-"+expiry_str)

              roll_cost_value.append({
                'symbol': symbol,
                'contract': symbol+"-USDT-"+expiry_str,
                'prompt': prompt,
                'roll_cost': (spot_bid - linear_ask) / spot_bid
              })
              roll_cost_value.append({
                'symbol': symbol,
                'contract': symbol+"-USD-"+expiry_str,
                'prompt': prompt,
                'roll_cost': (spot_bid - inverse_ask) / spot_bid
              })

            elif exchange == "binance":
              expiry_str = expiry_date.strftime('%y%m%d')
              print(exchange + " " + symbol + " " + expiry_str)

              linear_ask = Helper().get_linear_ask(exch=exch, symbol=symbol+"USDT_"+expiry_str)
              spot_bid = Helper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")['bid']
              inverse_ask = Helper().get_inverse_ask(exch=exch, symbol=symbol+"USD_"+expiry_str)

              roll_cost_value.append({
                'symbol': symbol,
                'contract': symbol+"USDT_"+expiry_str,
                'prompt': prompt,
                'roll_cost': (spot_bid - linear_ask) / spot_bid
              })
              roll_cost_value.append({
                'symbol': symbol,
                'contract': symbol+"USD_"+expiry_str,
                'prompt': prompt,
                'roll_cost': (spot_bid - inverse_ask) / spot_bid
              })
      
            elif exchange == "bybit":
              expiry_str = expiry_date.strftime('%d%b%y')
              print(exchange + " " + symbol + " " + expiry_str)

              linear_ask = BybitHelper().get_linear_ask(exch=exch, symbol=symbol+"-"+expiry_str)
              spot_bid = BybitHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")['bid']
              inverse_ask = BybitHelper().get_inverse_ask(exch=exch, symbol=symbol+"-"+expiry_str)

              roll_cost_value.append({
                'symbol': symbol,
                'contract': symbol+"-"+expiry_str,
                'prompt': prompt,
                'roll_cost': (spot_bid - linear_ask) / spot_bid
              })
              roll_cost_value.append({
                'symbol': symbol,
                'contract': symbol+"-"+expiry_str,
                'prompt': prompt,
                'roll_cost': (spot_bid - inverse_ask) / spot_bid
              })

          except ccxt.NetworkError as e:
            logger.warning(exchange +" roll costs " + symbol + " " + prompt + ": " + str(e))
            return False
          except Exception as e:
            logger.warning(exchange +" roll costs " + symbol + " " + prompt + ": " + str(e))

    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass

    roll_costs = []

    for item in roll_cost_value:
      new_value = {
        "venue": exchange,
        "roll_cost_value": item, 
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

      roll_costs.append(new_value)

    try:
      self.roll_costs_db.insert_many(roll_costs)

      return True
    
    except Exception as e:
      logger.error(exchange +" roll costs " + str(e))
      return True