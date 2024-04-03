from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.lib.expiry_date import get_expiry_date, get_prompt_month_code_from_expiry_date
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
from src.config import read_config_file

config = read_config_file()

class Roll_Costs:
  def __init__(self, db, collection):

    self.runs_db = db['runs']
    self.roll_costs_db = db['roll_costs']
    self.tickers_db = db['tickers']

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
    roll_cost_values: str = None,
    logger=None
  ):
    if exch == None:
      exch = Exchange(exchange).exch()

    if roll_cost_values is None:
      query = {'venue': exchange}
      ticker_values = self.tickers_db.find(query)

      for item in ticker_values:
        ticker_value = item["ticker_value"]

      roll_cost_values = []

      for symbol in config['roll_costs']['symbols']:
        for prompt in config['roll_costs']['prompts']:
          expiry_date = get_expiry_date(prompt, datetime.now(timezone.utc))
          
          try:
            if exchange == "okx":
              expiry_str = expiry_date.strftime('%y%m%d')

              linear_prices = OKXHelper().get_future_prices(exch=exch, symbol=symbol+"-USDT-"+expiry_str)
              spot_bid = OKXHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")['bid']
              inverse_prices = OKXHelper().get_future_prices(exch=exch, symbol=symbol+"-USD-"+expiry_str)

              linear_oi = OKXHelper().get_open_interests(exch=exch, symbol=symbol+"-USDT-"+expiry_str)
              inverse_oi = OKXHelper().get_open_interests(exch=exch, symbol=symbol+"-USD-"+expiry_str)

              linear_ticker = OKXHelper().get_ticker(exch=exch, symbol=symbol+"-USDT-"+expiry_str)
              inverse_ticker = OKXHelper().get_ticker(exch=exch, symbol=symbol+"-USD-"+expiry_str)

              roll_cost_values.append({
                'symbol': symbol,
                'contract': symbol+"-USDT-"+expiry_str,
                'prompt': prompt,
                'carry_cost': (float(linear_prices[2]) - spot_bid) / spot_bid,
                'term_structure': (float(linear_prices[2]) + float(linear_prices[3])) / 2,
                'open_interests': {
                  'oi': linear_oi['openInterestAmount'] * config['roll_costs']['contract_values'][exchange][symbol+"_LINEAR"] * linear_ticker['last'],
                  'volume': config['roll_costs']['contract_values'][exchange][symbol+"_LINEAR"] * linear_ticker['baseVolume'] * linear_ticker['last']
                },
                'expiry': expiry_date,
                'type': "linear"
              })
              roll_cost_values.append({
                'symbol': symbol,
                'contract': symbol+"-USD-"+expiry_str,
                'prompt': prompt,
                'carry_cost': (float(inverse_prices[2]) - spot_bid) / spot_bid,
                'term_structure': (float(inverse_prices[2]) + float(inverse_prices[3])) / 2,
                'open_interests': {
                  'oi': inverse_oi['openInterestAmount'] * config['roll_costs']['contract_values'][exchange][symbol+"_INVERSE"],
                  'volume': config['roll_costs']['contract_values'][exchange][symbol+"_INVERSE"] * inverse_ticker['baseVolume']
                },
                'expiry': expiry_date,
                'type': "inverse"
              })

            elif exchange == "binance":
              expiry_str = expiry_date.strftime('%y%m%d')

              linear_prices = Helper().get_linear_prices(exch=exch, symbol=symbol+"USDT_"+expiry_str)
              spot_bid = Helper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")['bid']
              inverse_prices = Helper().get_inverse_prices(exch=exch, symbol=symbol+"USD_"+expiry_str)

              linear_oi = Helper().get_open_interests(exch=exch, symbol=symbol+"USDT_"+expiry_str)
              inverse_oi = Helper().get_open_interests(exch=exch, symbol=symbol+"USD_"+expiry_str)

              linear_ticker = Helper().get_ticker(exch=exch, symbol=symbol+"USDT_"+expiry_str)
              inverse_ticker = Helper().get_ticker(exch=exch, symbol=symbol+"USD_"+expiry_str)

              roll_cost_values.append({
                'symbol': symbol,
                'contract': symbol+"USDT_"+expiry_str,
                'prompt': prompt,
                'carry_cost': (float(linear_prices['askPrice']) - spot_bid) / spot_bid,
                'term_structure': (float(linear_prices['bidPrice']) + float(linear_prices['askPrice'])) / 2,
                'open_interests': {
                  'oi': linear_oi['openInterestAmount'] * config['roll_costs']['contract_values'][exchange][symbol+"_LINEAR"] * linear_ticker['last'],
                  'volume': config['roll_costs']['contract_values'][exchange][symbol+"_LINEAR"] * linear_ticker['quoteVolume']
                },
                'expiry': expiry_date,
                'type': "linear"
              })
              roll_cost_values.append({
                'symbol': symbol,
                'contract': symbol+"USD_"+expiry_str,
                'prompt': prompt,
                'carry_cost': (float(inverse_prices['askPrice'])  - spot_bid) / spot_bid,
                'term_structure': (float(inverse_prices['bidPrice']) + float(inverse_prices['askPrice'])) / 2,
                'open_interests': {
                  'oi': inverse_oi['openInterestAmount'] * config['roll_costs']['contract_values'][exchange][symbol+"_INVERSE"],
                  'volume': config['roll_costs']['contract_values'][exchange][symbol+"_INVERSE"] * inverse_ticker['quoteVolume']
                },
                'expiry': expiry_date,
                'type': "inverse"
              })
      
            elif exchange == "bybit":
              expiry_str = expiry_date.strftime('%d%b%y').upper()

              linear_prices = BybitHelper().get_linear_prices(exch=exch, symbol=symbol+"-"+expiry_str)
              spot_bid = BybitHelper().get_bid_ask(exch=exch, symbol=symbol+"/USDT")['bid']
              inverse_prices = BybitHelper().get_inverse_prices(exch=exch, symbol=symbol+"-"+expiry_str)

              roll_cost_values.append({
                'symbol': symbol,
                'contract': symbol+"-"+expiry_str,
                'prompt': prompt,
                'carry_cost': (float(linear_prices['ask1Price']) - spot_bid) / spot_bid,
                'term_structure': (float(linear_prices['bid1Price']) + float(linear_prices['ask1Price'])) / 2,
                'expiry': expiry_date,
                'type': "linear"
              })
              roll_cost_values.append({
                'symbol': symbol,
                'contract': symbol+"-"+expiry_str,
                'prompt': prompt,
                'carry_cost': (float(inverse_prices['ask1Price'])  - spot_bid) / spot_bid,
                'term_structure': (float(inverse_prices['bid1Price']) + float(inverse_prices['ask1Price'])) / 2,
                'expiry': expiry_date,
                'type': "inverse"
              })

              try:
                linear_oi = BybitHelper().get_open_interests(
                  exch=exch, symbol=symbol+"-"+expiry_str,
                  params={'intervalTime': "5min", 'limit': 1}
                )
                inverse_oi = BybitHelper().get_open_interests(
                  exch=exch, symbol=symbol+"USD"+get_prompt_month_code_from_expiry_date(expiry_date),
                  params={'intervalTime': "5min", 'limit': 1, 'category': "inverse"}
                )

                linear_ticker = BybitHelper().get_ticker(exch=exch, symbol=symbol+"-"+expiry_str)
                inverse_ticker = BybitHelper().get_ticker(exch=exch, symbol=symbol+"USD"+get_prompt_month_code_from_expiry_date(expiry_date))

                roll_cost_values[-2]['open_interests'] = {
                  'oi': linear_oi['openInterestValue'] * config['roll_costs']['contract_values'][exchange][symbol+"_LINEAR"] * linear_ticker['last'],
                  'volume': config['roll_costs']['contract_values'][exchange][symbol+"_LINEAR"] * linear_ticker['quoteVolume']
                }
                roll_cost_values[-1]['open_interests'] = {
                  'oi': inverse_oi['openInterestValue'] * config['roll_costs']['contract_values'][exchange][symbol+"_INVERSE"],
                  'volume': config['roll_costs']['contract_values'][exchange][symbol+"_INVERSE"] * inverse_ticker['baseVolume']
                }
              except:
                pass

          except ccxt.NetworkError as e:
            if logger == None:
              print(exchange +" roll costs " + symbol + " " + prompt + ": " + str(e))
            else:
              logger.warning(exchange +" roll costs " + symbol + " " + prompt + ": " + str(e))

            return False

          except Exception as e:
            if logger == None:
              print(exchange +" roll costs " + symbol + " " + prompt + ": " + str(e))
            else:
              logger.warning(exchange +" roll costs " + symbol + " " + prompt + ": " + str(e))

    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass

    roll_costs = []

    for item in roll_cost_values:
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

    del roll_cost_values

    try:
      self.roll_costs_db.insert_many(roll_costs)

      del roll_costs

      if logger == None:
        print("Collected roll costs for " + exchange)
      else:
        logger.info("Collected roll costs for " + exchange)

      return True
    
    except Exception as e:
      if logger == None:
        print(exchange +" carry costs " + str(e))
        print("Unable to collect roll costs for " + exchange)
      else:
        logger.error(exchange +" carry costs " + str(e))
        logger.error("Unable to collect roll costs for " + exchange)

      return True