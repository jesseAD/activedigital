from datetime import datetime, timezone, timedelta
import time
import ccxt
# import pdb
from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.lib.unhedged import get_unhedged
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper, DeribitHelper

config = read_config_file()


class Positions:
  def __init__(self, db, collection):

    self.runs_db = db[config['mongodb']['database']]['runs']
    self.tickers_db = db[config['mongodb']['database']]['tickers']
    self.instruments_db = db[config['mongodb']['database']]['instruments']
    self.balances_db = db[config['mongodb']['database']]['balances']
    self.funding_rates_db = db[config['mongodb']['database']]['funding_rates']
    self.lifetime_funding_db = db[config['mongodb']['database']]['lifetime_funding']
    self.split_positions_db = db[config['mongodb']['database']]['split_positions']
    self.positions_db = db[config['mongodb']['database']][collection]
    self.mark_prices_db = db[config['mongodb']['database']]['mark_prices']
    self.price_changes_db = db[config['mongodb']['database']]['open_positions_price_change']

    self.session = db.start_session()

  def __del__(self):
    self.session.end_session()

  # def get(
  #   self,
  #   client,
  #   active: bool = None,
  #   spot: str = None,
  #   future: str = None,
  #   perp: str = None,
  #   position_type: str = None,
  #   exchange: str = None,
  #   account: str = None,
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
  #   if position_type:
  #     pipeline.append({"$match": {"positionType": position_type}})
  #   if client:
  #     pipeline.append({"$match": {"client": client}})
  #   if exchange:
  #     pipeline.append({"$match": {"venue": exchange}})
  #   if account:
  #     pipeline.append({"$match": {"account": account}})

  #   try:
  #     results = self.positions_db.aggregate(pipeline)
  #     return results

  #   except Exception as e:
  #     log.error(e)

  def create(
    self,
    client,
    exch=None,
    exchange: str = None,
    sub_account: str = None,
    spot: str = None,
    future: str = None,
    perp: str = None,
    position_value: str = None,
    logger=None,
    secrets={},
    balance_finished={},
  ):
    self.session.start_transaction()

    vip_level = ""

    if position_value is None:
      while(not balance_finished[client + "_" + exchange + "_" + sub_account]):
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " positions: balances was not finished")
        else:
          logger.info(client + " " + exchange + " " + sub_account + " positions: balances was not finished")

        time.sleep(1)

      if exch == None:
        spec = (client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_")
        API_KEY = secrets[spec + "API_KEY"]
        API_SECRET = secrets[spec + "API_SECRET"]
        PASSPHRASE = None
        if exchange == "okx":
          PASSPHRASE = secrets[spec + "PASSPHRASE"]

        exch = Exchange(
          exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE
        ).exch()

      try:
        if exchange == "okx":
          position_value = OKXHelper().get_positions(exch=exch)

        elif exchange == "binance":
          if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
            position_value = Helper().get_pm_positions(exch=exch)
            for item in position_value:
              item['info'] = {**item}
              item['marginMode'] = "cross"
          else:
            position_value = Helper().get_positions(exch=exch)
            for item in position_value:
              item['info'] = {**item}
              item['marginMode'] = "cross"

        elif exchange == "bybit":
          position_value = BybitHelper().get_positions(exch=exch)

        elif exchange == "huobi":
          position_value = HuobiHelper().get_positions(exch=exch)

        elif exchange == "deribit":
          position_value = DeribitHelper().get_positions(exch=exch)
          for item in position_value:
            item['info']['symbol'] = item['info']['instrument_name']

            if "delta" in item['info']:
              item['delta'] = float(item['info']['delta'])
            if "vega" in item['info']:
              item['vega'] = float(item['info']['vega'])
            if "theta" in item['info']:
              item['theta'] = float(item['info']['theta'])
            if "gamma" in item['info']:
              item['gamma'] = float(item['info']['gamma'])

        position_value = Mapping().mapping_positions(exchange=exchange, positions=position_value)

      except ccxt.ExchangeError as e:
        self.session.abort_transaction()

        if logger == None:
          print(client + " " + exchange + " " + sub_account + " positions " + str(e))
          print("Unable to collect positions for " + client + " " + exchange + " " + sub_account)
        else:
          logger.error(client + " " + exchange + " " + sub_account + " positions " + str(e))
          logger.error("Unable to collect positions for " + client + " " + exchange + " " + sub_account)

        return True
      
      except ccxt.NetworkError as e:
        self.session.abort_transaction()

        if logger == None:
          print(client + " " + exchange + " " + sub_account + " positions " + str(e))
        else:
          logger.error(client + " " + exchange + " " + sub_account + " positions " + str(e))

        return False
      
      except Exception as e:
        self.session.abort_transaction()

        if logger == None:
          print(client + " " + exchange + " " + sub_account + " positions " + str(e))
        else:
          logger.error(client + " " + exchange + " " + sub_account + " positions " + str(e))

        return False
        
      position_info = []
      liquidation_buffer = 1
      tickers = list(self.tickers_db.find({"venue": exchange}))[0]["ticker_value"]

      if exchange == "okx":
        try:
          cross_margin_ratio = float(
            OKXHelper().get_cross_margin_ratio(exch=exch)
          )
          liquidation_buffer = OKXHelper().calc_liquidation_buffer(
            exchange=exchange, mgnRatio=cross_margin_ratio
          )

        except ccxt.ExchangeError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            
          return True
        
        except ccxt.NetworkError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            
          return False
        
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

      elif exchange == "bybit":
        try:
          cross_margin_ratio = float(
            BybitHelper().get_cross_margin_ratio(exch=exch)
          )
          liquidation_buffer = BybitHelper().calc_liquidation_buffer(
            exchange=exchange, mgnRatio=cross_margin_ratio
          )

        except ccxt.ExchangeError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

          return True
        
        except ccxt.NetworkError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            
          return False
        
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

      elif exchange == "deribit":
        try:
          cross_margin_ratio = float(
            DeribitHelper().get_cross_margin_ratio(exch=exch)
          )
          liquidation_buffer = DeribitHelper().calc_liquidation_buffer(
            exchange=exchange, mgnRatio=cross_margin_ratio
          )

        except ccxt.ExchangeError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

          return True
        
        except ccxt.NetworkError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            
          return False
        
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

      elif exchange == "huobi":
        try:
          liquidation1 = HuobiHelper().calc_liquidation_buffer(
            exchange=exchange, mgnRatio=float(HuobiHelper().get_cm_cross_margin_ratio(exch=exch))
          )
          liquidation2 = HuobiHelper().calc_liquidation_buffer(
            exchange=exchange, mgnRatio=float(HuobiHelper().get_um_cross_margin_ratio(exch=exch))
          )
          liquidation_buffer = min(liquidation1, liquidation2)

        except ccxt.ExchangeError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

          return True
        
        except ccxt.NetworkError as e:
          self.session.abort_transaction()

          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            
          return False
        
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

      elif exchange == "binance":
        if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
          try:
            cross_margin_ratio = float(
              Helper().get_pm_cross_margin_ratio(exch=exch)
            )
            liquidation_buffer = Helper().calc_liquidation_buffer(
              exchange=exchange, mgnRatio=cross_margin_ratio
            )

          except ccxt.ExchangeError as e:
            self.session.abort_transaction()

            if logger == None:
              print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

            return True
          
          except ccxt.NetworkError as e:
            self.session.abort_transaction()

            if logger == None:
              print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
              
            return False
          
          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

        else:
          try:
            liquidation1 = Helper().calc_liquidation_buffer(
              exchange=exchange, mgnRatio=Helper().get_cross_margin_ratio(exch=exch)
            )
            liquidation2 = Helper().calc_liquidation_buffer(
              exchange=exchange+"_cm", mgnRatio=Helper().get_cm_margin_ratio(exch=exch)
            )
            liquidation3 = Helper().calc_liquidation_buffer(
              exchange=exchange+"_um", mgnRatio=Helper().get_um_margin_ratio(exch=exch)
            )
            liquidation_buffer = min(liquidation1, liquidation2, liquidation3)

          except ccxt.ExchangeError as e:
            self.session.abort_transaction()

            if logger == None:
              print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

            return True
          
          except ccxt.NetworkError as e:
            self.session.abort_transaction()

            if logger == None:
              print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
              
            return False
          
          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " positions: in cross margin ratio " + str(e))

      try:
        instruments = list(self.instruments_db.find({'venue': exchange}))[0]['instrument_value']
      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " positions skipped as non instruments")
          print("Unable to collect positions for " + client + " " + exchange + " " + sub_account)
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " positions skipped as non instruments")
          logger.error("Unable to collect positions for " + client + " " + exchange + " " + sub_account)

        return True

      for value in position_value:
        try:
          value['base'] = instruments[value['info']['symbol']]['base']
          value['quote'] = instruments[value['info']['symbol']]['quote']
          if instruments[value['info']['symbol']]['expiryDatetime'] != None and instruments[value['info']['symbol']]['expiryDatetime'] != "":
            value['expiryDatetime'] = (
              datetime.strptime(instruments[value['info']['symbol']]['expiryDatetime'], '%Y-%m-%dT%H:%M:%S.%f%z') - 
              datetime.now(timezone.utc)
            ).days

        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions skipped" + " as non instruments")
            print("Unable to collect positions for " + client + " " + exchange + " " + sub_account)
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions skipped" + " as non instruments")
            logger.error("Unable to collect positions for " + client + " " + exchange + " " + sub_account)

          return True

        try:
          if exchange == "bybit":
            value["liquidationBuffer"] = liquidation_buffer
            position_info.append(value)

          elif exchange == "deribit":
            value["liquidationBuffer"] = liquidation_buffer
            value["notional"] = value['contracts'] * tickers[value['base'] + "/USDT"]['last']
            position_info.append(value)

          elif exchange == "okx":
            value["liquidationBuffer"] = liquidation_buffer

            if value["quote"] == "USD":
              cross_ratio = Helper().calc_cross_ccy_ratio(
                value["base"],
                # config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"],
                "USD",
                tickers,
              )
              if cross_ratio == 0:
                if logger == None:
                  print(client + " " + exchange + " " + sub_account + " positions skipped" + value['symbol'] + "as zero ticker price")
                else:
                  logger.warning(client + " " + exchange + " " + sub_account + " positions skipped" + value['symbol'] + "as zero ticker price")

                continue

              value["notional"] = float(value["notional"]) * cross_ratio
              value["unrealizedPnl"] = float(value["unrealizedPnl"]) * cross_ratio
            
            position_info.append(value)

          elif exchange == "huobi":
            value["liquidationBuffer"] = liquidation_buffer

            if value["quote"] == "USD":
              cross_ratio = Helper().calc_cross_ccy_ratio(
                value["base"],
                # config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"],
                "USD",
                tickers,
              )
              if cross_ratio == 0:
                if logger == None:
                  print(client + " " + exchange + " " + sub_account + " positions skipped" + value['symbol'] + "as zero ticker price")
                else:
                  logger.warning(client + " " + exchange + " " + sub_account + " positions skipped" + value['symbol'] + "as zero ticker price")

                continue

              value["notional"] = float(value["notional"]) * cross_ratio
              value["unrealizedPnl"] = float(value["unrealizedPnl"]) * cross_ratio

            position_info.append(value)

          elif exchange == "binance":
            if (float(value["initialMargin"]) != 0.0 if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'non_portfolio' else True):
              value['side'] = "long" if float(value['contracts']) > 0 else "short"
              
              value["liquidationBuffer"] = liquidation_buffer

              if value["quote"] == "USD":
                cross_ratio = Helper().calc_cross_ccy_ratio(
                  value["base"],
                  # config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"],
                  "USD",
                  tickers,
                )
                if cross_ratio == 0:
                  if logger == None:
                    print(client + " " + exchange + " " + sub_account + " positions skipped" + value['symbol'] + "as zero ticker price")
                  else:
                    logger.warning(client + " " + exchange + " " + sub_account + " positions skipped" + value['symbol'] + "as zero ticker price")

                  continue
                
                value["notional"] = float(value["notional"]) * cross_ratio
                value["unrealizedPnl"] = float(value["unrealizedPnl"]) * cross_ratio

              position_info.append(value)

        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " positions " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
      
        if exchange == "binance" or exchange == "huobi":
          for position in position_info:
            try:
              mark_prices = self.mark_prices_db.aggregate([
                {
                  '$match': {
                    '$expr': {
                      '$and': [
                        {
                          '$eq': [
                            '$venue', exchange
                          ]
                        }, {
                          '$eq': [
                            '$symbol', position['base'] + "/USDT"
                          ]
                        }
                      ]
                    }
                  }
                }, {
                  '$sort': {'timestamp': -1}
                }, {
                  '$limit': 1
                }
              ])
              for item in mark_prices:
                try:
                  mark_price = item['mark_price_value']['markPrice']
                except:
                  pass
                
              position["markPrice"] = mark_price
            except ccxt.ExchangeError as e:
              if logger == None:
                print(client + " " + exchange + " " + sub_account + " positions " + str(e))
              else:
                logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))

              pass
        
      # get vip level

      try:
        if exchange == "deribit": 
          vip_level = config["clients"][client]["subaccounts"][exchange][sub_account]["vip_level"]

        elif exchange == "huobi":
          vip_level = config["clients"][client]["subaccounts"][exchange][sub_account]["vip_level"]

        elif exchange == "okx":
          vip_level = OKXHelper().get_vip_level(exch)

        elif exchange == "binance":
          vip_level = Helper().get_vip_level(exch)

        elif exchange == "bybit":
          vip_level = BybitHelper().get_vip_level(exch)

      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " vip level: " + str(e))
          print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
        else:
          logger.error(client + " " + exchange + " " + sub_account + " vip level: " + str(e))
          logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)

      run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
      latest_run_id = 0
      for item in run_ids:
        try:
          latest_run_id = item["runid"]
        except:
          pass

      # open positions price changes

      price_changes = []
      for position in position_info:
        try:
          ticker = float(tickers[position['base'] + "/USDT"]['last'])
          mark_klines = exch.fetch_mark_ohlcv(symbol = position['base'] + "/USDT", timeframe = '2h', limit=1)

          price_change = {
            'client': client,
            'venue': exchange,
            'account': sub_account,
            'tier': vip_level,
            'base': position['base'],
            'symbol': position['symbol'],
            'price_change': (ticker - mark_klines[0][1]) / mark_klines[0][1] * 100,
            'runid': latest_run_id,
            'timestamp': datetime.now(timezone.utc)
          }
          price_changes.append(price_change)

        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " price changes " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " price changes " + str(e))

          pass

      try:
        if len(price_changes) > 0:
          self.price_changes_db.insert_many(price_changes)
        else:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " empty price changes")
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " empty price changes")

      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " price changes " + str(e))
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " price changes " + str(e))

        pass

      del price_changes

    else:
      position_info = position_value

      run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
      latest_run_id = 0
      for item in run_ids:
        try:
          latest_run_id = item["runid"]
        except:
          pass

    # liquidation price change

    for position in position_info:
      try:
        position['liquidationPriceChange'] = (float(position['liquidationPrice']) - float(position['markPrice'])) / float(position['markPrice'])
      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " positions in liquidation price change: " + str(e))
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " positions in liquidation price change: " + str(e))
    
    

    

    # life time funding rates

    query = {}
    query['client'] = client
    query['venue'] = exchange
    query['account'] = sub_account

    lifetime_funding_values = list(self.lifetime_funding_db.find(query))
    lifetime_funding_values.sort(key = lambda x: x['symbol'])
    position_info.sort(key = lambda x: x['symbol'])

    fundings = []
    current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    i = 0
    j = 0
    while True:
      try:
        if i == len(position_info) and j == len(lifetime_funding_values):
          break
        if i == len(position_info):
          funding = {
            'client': client,
            'venue': exchange,
            'account': sub_account,
            'symbol': lifetime_funding_values[j]['symbol'],
            'funding': lifetime_funding_values[j]['funding'],
            'state': "closed",
            'open_close_time': lifetime_funding_values[j]['open_close_time'],
            'last_time': lifetime_funding_values[j]['last_time'],
            'base': lifetime_funding_values[j]['base'],
            'quote': lifetime_funding_values[j]['quote'],
          }
          if lifetime_funding_values[j]['state'] == "closed":
            if lifetime_funding_values[j]['open_close_time'] > lifetime_funding_values[j]['last_time']:
              funding_rates = self.funding_rates_db.aggregate([
                {
                  '$match': {
                    '$expr': {
                      '$and': [
                        {
                          '$eq': [
                            '$venue', exchange
                          ]
                        }, {
                          '$eq': [
                            '$symbol', lifetime_funding_values[j]['base'] + "/USDT" if lifetime_funding_values[j]['quote'] == "USDT" else lifetime_funding_values[j]['base'] + "/USD"
                          ]
                        }
                      ]
                    }
                  }
                }, {
                  '$sort': {'funding_rates_value.timestamp': -1}
                }, {
                  '$limit': 1
                }
              ])
              funding_rate = 0.0
              funding_time = 0
              for item in funding_rates:
                funding_rate = item['funding_rates_value']['fundingRate']
                funding_time = item['funding_rates_value']['timestamp']

              if funding_time > lifetime_funding_values[j]['last_time']:
                funding['last_time'] = lifetime_funding_values[j]['open_close_time']
                funding['funding'] += (funding_rate * (lifetime_funding_values[j]['open_close_time'] - lifetime_funding_values[j]['last_time']) / 28800000)
          else:
            funding['open_close_time'] = current_time

          fundings.append(funding)
          j += 1

        elif j == len(lifetime_funding_values):
          fundings.append({
            'client': client,
            'venue': exchange,
            'account': sub_account,
            'symbol': position_info[i]['symbol'],
            'funding': 0.0,
            'state': "open",
            'open_close_time': current_time,
            'last_time': current_time,
            'base': position_info[i]['base'],
            'quote': position_info[i]['quote']
          })
          position_info[i]['lifetime_funding_rates'] = 0.0
          i += 1

        else:
          if position_info[i]['symbol'] == lifetime_funding_values[j]['symbol']:
            if lifetime_funding_values[j]['state'] == "closed":
              if lifetime_funding_values[j]['open_close_time'] > lifetime_funding_values[j]['last_time']:
                fundings.append({
                  'client': client,
                  'venue': exchange,
                  'account': sub_account,
                  'symbol': lifetime_funding_values[j]['symbol'],
                  'funding': lifetime_funding_values[j]['funding'],
                  'state': "open",
                  'open_close_time': current_time - lifetime_funding_values[j]['open_close_time'] + lifetime_funding_values[j]['last_time'],
                  'last_time': lifetime_funding_values[j]['last_time'],
                  'base': lifetime_funding_values[j]['base'],
                  'quote': lifetime_funding_values[j]['quote']
                })
              else:
                fundings.append({
                  'client': client,
                  'venue': exchange,
                  'account': sub_account,
                  'symbol': lifetime_funding_values[j]['symbol'],
                  'funding': lifetime_funding_values[j]['funding'],
                  'state': "open",
                  'open_close_time': current_time,
                  'last_time': current_time,
                  'base': lifetime_funding_values[j]['base'],
                  'quote': lifetime_funding_values[j]['quote']
                })
              position_info[i]['lifetime_funding_rates'] = lifetime_funding_values[j]['funding']
            else:
              funding_rates = self.funding_rates_db.aggregate([
                {
                  '$match': {
                    '$expr': {
                      '$and': [
                        {
                          '$eq': [
                            '$venue', exchange
                          ]
                        }, {
                          '$eq': [
                            '$symbol', position_info[i]['base'] + "/USDT" if position_info[i]['quote'] == "USDT" else position_info[i]['base'] + "/USD"
                          ]
                        }
                      ]
                    }
                  }
                }, {
                  '$sort': {'funding_rates_value.timestamp': -1}
                }, {
                  '$limit': 1
                }
              ])
              funding_rate = 0.0
              funding_time = 0
              for item in funding_rates:
                funding_rate = item['funding_rates_value']['fundingRate']
                funding_time = item['funding_rates_value']['timestamp']

              if lifetime_funding_values[j]['open_close_time'] < funding_time and lifetime_funding_values[j]['last_time'] < funding_time:
                fundings.append({
                  'client': client,
                  'venue': exchange,
                  'account': sub_account,
                  'symbol': position_info[i]['symbol'],
                  'funding': (lifetime_funding_values[j]['funding'] + funding_rate 
                        * (funding_time - max(lifetime_funding_values[j]['open_close_time'], lifetime_funding_values[j]['last_time'])) / 28800000),
                  'state': "open",
                  'open_close_time': lifetime_funding_values[j]['open_close_time'],
                  'last_time': funding_time,
                  'base': lifetime_funding_values[j]['base'],
                  'quote': lifetime_funding_values[j]['quote']
                })
                position_info[i]['lifetime_funding_rates'] = fundings[-1]['funding']
              else:
                position_info[i]['lifetime_funding_rates'] = lifetime_funding_values[j]['funding']

            i += 1
            j += 1

          elif position_info[i]['symbol'] < lifetime_funding_values[j]['symbol']:
            fundings.append({
              'client': client,
              'venue': exchange,
              'account': sub_account,
              'symbol': position_info[i]['symbol'],
              'funding': 0.0,
              'state': "open",
              'open_close_time': current_time,
              'last_time': current_time,
              'base': position_info[i]['base'],
              'quote': position_info[i]['quote']
            })
            position_info[i]['lifetime_funding_rates'] = 0.0
            i += 1

          elif position_info[i]['symbol'] > lifetime_funding_values[j]['symbol']:
            funding = {
              'client': client,
              'venue': exchange,
              'account': sub_account,
              'symbol': lifetime_funding_values[j]['symbol'],
              'funding': lifetime_funding_values[j]['funding'],
              'state': "closed",
              'open_close_time': lifetime_funding_values[j]['open_close_time'],
              'last_time': lifetime_funding_values[j]['last_time'],
              'base': lifetime_funding_values[j]['base'],
              'quote': lifetime_funding_values[j]['quote'],
            }
            if lifetime_funding_values[j]['state'] == "closed":
              if lifetime_funding_values[j]['open_close_time'] > lifetime_funding_values[j]['last_time']:
                funding_rates = self.funding_rates_db.aggregate([
                  {
                    '$match': {
                      '$expr': {
                        '$and': [
                          {
                            '$eq': [
                              '$venue', exchange
                            ]
                          }, {
                            '$eq': [
                              '$symbol', lifetime_funding_values[j]['base'] + "/USDT" if lifetime_funding_values[j]['quote'] == "USDT" else lifetime_funding_values[j]['base'] + "/USD"
                            ]
                          }
                        ]
                      }
                    }
                  }, {
                    '$sort': {'funding_rates_value.timestamp': -1}
                  }, {
                    '$limit': 1
                  }
                ])
                funding_rate = 0.0
                funding_time = 0
                for item in funding_rates:
                  funding_rate = item['funding_rates_value']['fundingRate']
                  funding_time = item['funding_rates_value']['timestamp']

                if funding_time > lifetime_funding_values[j]['last_time']:
                  funding['last_time'] = lifetime_funding_values[j]['open_close_time']
                  funding['funding'] += (funding_rate * (lifetime_funding_values[j]['open_close_time'] - lifetime_funding_values[j]['last_time']) / 28800000)
            else:
              funding['open_close_time'] = current_time

            fundings.append(funding)
            j += 1

      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " lifetime fundings " + str(e))
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " lifetime fundings " + str(e))

    for item in fundings:
      try:
        self.lifetime_funding_db.update_one(
          {
            'client': client,
            'venue': exchange,
            'account': sub_account,
            'symbol': item['symbol']
          },
          {"$set": {
            'state': item['state'],
            'funding': item['funding'],
            'open_close_time': item['open_close_time'],
            'last_time': item['last_time'],
            'base': item['base'],
            'quote': item['quote']
          }},
          upsert=True
        )
      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " lifetime fundings " + str(e))
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " lifetime fundings " + str(e))

        # print("An error occurred in Lifetime Funding:", e)

    # calculate unhedged

    if config['clients'][client]['split_positions'] == True:
      try:
        balance_values = self.balances_db.aggregate([
          {
            '$match': {
              '$expr': {
                '$and': [
                  {
                    '$eq': [
                      '$client', client
                    ]
                  }, {
                    '$eq': [
                      '$venue', exchange
                    ]
                  }, {
                    '$eq': [
                      '$account', sub_account
                    ]
                  }, {
                    '$gt': [
                      '$timestamp', datetime.now(timezone.utc) - timedelta(days=1)
                    ]
                  }
                ]
              }
            }
          }, {
            '$sort': {'timestamp': -1}
          }, {
            '$limit': 1
          }
        ])

        balance = None
        for item in balance_values:
          timestamp = item['timestamp']
          balance = item["balance_value"]

        spot_positions = []

        if balance != None:
          for _key, _val in balance.items():
            if (_key == "base" or 
                _key in config['ignore_symbols'][exchange] or 
                _key in config["clients"][client]["subaccounts"][exchange][sub_account]["ignore_symbols"]):
              
              continue

            spot_position = {}
            spot_position['base'] = _key
            spot_position['quote'] = _key
            spot_position['symbol'] = _key
            spot_position['contracts'] = _val
            spot_position['avgPrice'] = 0
            spot_position['leverage'] = 0
            spot_position['unrealizedPnl'] = 0
            spot_position['lifetime_funding_rates'] = 0
            spot_position['marginMode'] = None
            spot_position['timestamp'] = int(timestamp.timestamp() * 1000)
            spot_position['side'] = "long" if _val > 0 else "short"
            spot_position['markPrice'] = 1 if _key == "USDT" else tickers[_key + "/USDT"]['last']
            spot_position['notional'] = spot_position['markPrice'] * spot_position['contracts']

            spot_positions.append(spot_position)

        split_positions = []
        split_positions = get_unhedged(position_info, spot_positions)

        hedged_exclusion_positive = config['clients'][client]['subaccounts'][exchange][sub_account]['hedged_exclusion_positive']
        hedged_exclusion_negative = config['clients'][client]['subaccounts'][exchange][sub_account]['hedged_exclusion_negative']
        i = 0
        while i < len(split_positions):
          if len(split_positions[i]) == 1:
            if split_positions[i][0]['position'] > 0 and split_positions[i][0]['base'] in hedged_exclusion_positive:
              if hedged_exclusion_positive[split_positions[i][0]['base']] == 0:
                split_positions.pop(i)
                i -= 1
              else:
                split_positions[i][0]['notional'] *= (1 - hedged_exclusion_positive[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                split_positions[i][0]['unrealizedPnl'] *= (1 - hedged_exclusion_positive[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                split_positions[i][0]['unhedgedAmount'] *= (1 - hedged_exclusion_positive[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                split_positions[i][0]['position'] -= hedged_exclusion_positive[split_positions[i][0]['base']]

            elif split_positions[i][0]['position'] < 0 and split_positions[i][0]['base'] in hedged_exclusion_negative:
              if hedged_exclusion_negative[split_positions[i][0]['base']] == 0:
                split_positions.pop(i)
                i -= 1
              else:
                split_positions[i][0]['notional'] *= (1 + hedged_exclusion_negative[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                split_positions[i][0]['unrealizedPnl'] *= (1 + hedged_exclusion_negative[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                split_positions[i][0]['unhedgedAmount'] *= (1 + hedged_exclusion_negative[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                split_positions[i][0]['position'] += hedged_exclusion_negative[split_positions[i][0]['base']]

          i += 1
        
        current_time = datetime.now(timezone.utc)
        split_position = {
          "client": client,
          "venue": exchange,
          "account": "Main Account",
          "tier": vip_level,
          "position_value": split_positions,
          "threshold": config['clients'][client]['hedged_threshold'] if "hedged_threshold" in config['clients'][client] else config['hedged']['threshold'],
          "unhedged_alert": config['clients'][client]['subaccounts'][exchange][sub_account]['unhedged_alert'],
          "active": True,
          "entry": False,
          "exit": False,
          "timestamp": current_time,
        }
        if sub_account:
          split_position["account"] = sub_account
        if spot:
          split_position["spotMarket"] = spot
        if future:
          split_position["futureMarket"] = future
        if perp:
          split_position["perpMarket"] = perp
        
        split_position["runid"] = latest_run_id

        self.split_positions_db.insert_one(split_position)
        
      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " split positions " + str(e))
          print("Unable to collect positions for " + client + " " + exchange + " " + sub_account)
        else:
          logger.error(client + " " + exchange + " " + sub_account + " split positions " + str(e))
          logger.error("Unable to collect positions for " + client + " " + exchange + " " + sub_account)

    current_time = datetime.now(timezone.utc)
    position = {
      "client": client,
      "venue": exchange,
      "account": "Main Account",
      "tier": vip_level,
      "position_value": position_info,
      "alert_threshold": config['positions']['alert_threshold'],
      "active": True,
      "entry": False,
      "exit": False,
      "timestamp": current_time,
    }

    del position_info

    if sub_account:
      position["account"] = sub_account
    if spot:
      position["spotMarket"] = spot
    if future:
      position["futureMarket"] = future
    if perp:
      position["perpMarket"] = perp

    position["runid"] = latest_run_id

    if exchange == "deribit":
      position['options_alerting'] = config['positions']['options_alerting']

    try:
      if config["positions"]["store_type"] == "timeseries":
        self.positions_db.insert_one(position)
      elif config["positions"]["store_type"] == "snapshot":
        self.positions_db.update_one(
          {
            "client": position["client"],
            "venue": position["venue"],
            "account": position["account"],
          },
          {
            "$set": {
              "position_value": position["position_value"],
              "active": position["active"],
              "entry": position["entry"],
              "exit": position["exit"],
              "timestamp": position["timestamp"],
              "runid": position["runid"],
            }
          },
          upsert=True,
        )

      self.session.commit_transaction()

      del position

      if logger == None:
        print("Collected positions for " + client + " " + exchange + " " + sub_account)
      else:
        logger.info("Collected positions for " + client + " " + exchange + " " + sub_account)

      return True

    except Exception as e:
      self.session.abort_transaction()

      if logger == None:
        print(client + " " + exchange + " " + sub_account + " positions " + str(e))
        print("Unable to collect positions for " + client + " " + exchange + " " + sub_account)
      else:
        logger.error(client + " " + exchange + " " + sub_account + " positions " + str(e))
        logger.error("Unable to collect positions for " + client + " " + exchange + " " + sub_account)

      return True