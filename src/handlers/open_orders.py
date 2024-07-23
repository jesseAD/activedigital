from datetime import datetime, timezone, timedelta
import ccxt

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper, DeribitHelper
from src.lib.mapping import Mapping

config = read_config_file()


class OpenOrders:
  def __init__(self, db, collection):

    self.runs_db = db[config['mongodb']['database']]['runs']
    self.tickers_db = db[config['mongodb']['database']]['tickers']
    self.open_orders_db = db[config['mongodb']['database']]['open_orders']

  def create(
    self,
    client,
    exch=None,
    exchange: str = None,
    sub_account: str = None,
    spot: str = None,
    future: str = None,
    perp: str = None,
    openOrderValue: str = None,
    logger=None,
    secrets={},
  ):
    if openOrderValue is None:
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
          openOrderValue = OKXHelper().get_open_orders(exch=exch)

        if exchange == "deribit":
          openOrderValue = DeribitHelper().get_open_orders(exch=exch, params={'currency': "BTC"})
          openOrderValue += DeribitHelper().get_open_orders(exch=exch, params={'currency': "ETH"})
          openOrderValue += DeribitHelper().get_open_orders(exch=exch, params={'currency': "USDT"})
          openOrderValue += DeribitHelper().get_open_orders(exch=exch, params={'currency': "USDC"})

        elif exchange == "binance":
          openOrderValue = Helper().get_spot_open_orders(exch=exch)
          if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
            openOrderValue += Helper().get_cm_open_orders(exch=exch)
            openOrderValue += Helper().get_um_open_orders(exch=exch)
          else:
            openOrderValue += Helper().get_linear_open_orders(exch=exch)
            openOrderValue += Helper().get_inverse_open_orders(exch=exch)

          for order in openOrderValue:
            order['info'] = {**order}

          openOrderValue = Mapping().mapping_open_orders(exchange=exchange, open_orders=openOrderValue)
                
        elif exchange == "bybit":
          openOrderValue = []
          openOrderValue += BybitHelper().get_open_orders(exch=exch, params={})
          openOrderValue += BybitHelper().get_open_orders(exch=exch, params={'settleCoin': "USDC"})
          openOrderValue += BybitHelper().get_open_orders(exch=exch, params={'category': "spot"})
          openOrderValue += BybitHelper().get_open_orders(exch=exch, params={'category': "option"})

        elif exchange == "huobi":
          openOrderValue = []
          openOrderValue += HuobiHelper().get_open_orders(exch=exch)
          openOrderValue += HuobiHelper().get_cm_open_orders(exch=exch)
          openOrderValue += HuobiHelper().get_future_open_orders(exch=exch)
          openOrderValue += HuobiHelper().get_cross_open_orders(exch=exch)
          openOrderValue += HuobiHelper().get_isolated_open_orders(exch=exch)

      except ccxt.ExchangeError as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " open orders " + str(e))
          print("Unable to collect open orders for " + client + " " + exchange + " " + sub_account)
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " open orders " + str(e))
          logger.error("Unable to collect open orders for " + client + " " + exchange + " " + sub_account)
           
        return True


    openOrderValue = [
      order for order in openOrderValue 
      if datetime.now(timezone.utc).timestamp() * 1000 - int(order['timestamp']) >= config['open_orders']['timeout'] * 1000
    ]

    query = {}
    if exchange:
      query["venue"] = exchange
    ticker_values = self.tickers_db.find(query)

    for item in ticker_values:
      ticker_value = item["ticker_value"]

    for order in openOrderValue:
      if exchange == "binance":
        try:
          order['current_price'] = Helper().get_ticker(exch=exch, symbol=order['symbol'])['last']
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in fetching ticker")
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in fetching ticker")

      elif exchange == "huobi":
        try:
          order['current_price'] = HuobiHelper().get_ticker(exch=exch, symbol=order['symbol'])['last']
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in fetching ticker")
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in fetching ticker")

      elif exchange == "deribit":
        try:
          order['current_price'] = DeribitHelper().get_ticker(exch=exch, symbol=order['symbol'])['last']
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in fetching ticker")
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in fetching ticker")

      else:
        try:
          order['current_price'] = ticker_value[order['symbol'].split(":")[0]]['last']
        except Exception as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in reading ticker")
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in reading ticker")

    open_orders = {
        "client": client,
        "venue": exchange,
        "account": "Main Account",
        "open_orders_value": openOrderValue,
        "timestamp": datetime.now(timezone.utc),
    }

    if sub_account:
        open_orders["account"] = sub_account
    if spot:
        open_orders["spotMarket"] = spot
    if future:
        open_orders["futureMarket"] = future
    if perp:
        open_orders["perpMarket"] = perp

    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
        try:
            latest_run_id = item["runid"]
        except:
            pass

    open_orders["runid"] = latest_run_id

    try:
      if config["open_orders"]["store_type"] == "timeseries":
        self.open_orders_db.insert_one(open_orders)
      elif config["open_orders"]["store_type"] == "snapshot":
        self.open_orders_db.update_one(
          {
            "client": open_orders["client"],
            "venue": open_orders["venue"],
            "account": open_orders["account"],
          },
          {
            "$set": {
              "open_orders_value": open_orders["open_orders_value"],
              "timestamp": open_orders["timestamp"],
              "runid": open_orders["runid"],
            }
          },
          upsert=True,
        )

      if logger == None:
        print("Collected open orders for " + client + " " + exchange + " " + sub_account)
      else:
        logger.info("Collected open orders for " + client + " " + exchange + " " + sub_account)
      
      return True
    
    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in persisting to database")
        print("Unable to collect open orders for " + client + " " + exchange + " " + sub_account)
      else:
        logger.error(client + " " + exchange + " " + sub_account + " open orders " + str(e) + " in persisting to database")
        logger.error("Unable to collect open orders for " + client + " " + exchange + " " + sub_account)
         
      return True

    