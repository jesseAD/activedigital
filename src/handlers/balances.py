from datetime import datetime, timezone, timedelta
import ccxt

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

config = read_config_file()


class Balances:
  def __init__(self, db, collection):

    self.runs_db = db[config['mongodb']['database']]['runs']
    self.tickers_db = db[config['mongodb']['database']]['tickers']
    self.balances_db = db[config['mongodb']['database']][collection]

  def get(
    self,
    active: bool = None,
    spot: str = None,
    future: str = None,
    perp: str = None,
    position_type: str = None,
    client: str = None,
    exchange: str = None,
    account: str = None,
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
    if position_type:
      pipeline.append({"$match": {"positionType": position_type}})
    if client:
      pipeline.append({"$match": {"client": client}})
    if exchange:
      pipeline.append({"$match": {"venue": exchange}})
    if account:
      pipeline.append({"$match": {"account": account}})

    try:
      results = self.balances_db.aggregate(pipeline)
      return results

    except Exception as e:
      print(e)

  def create(
    self,
    client,
    exch=None,
    exchange: str = None,
    sub_account: str = None,
    spot: str = None,
    future: str = None,
    perp: str = None,
    balanceValue: str = None,
    logger=None,
    secrets={},
  ):
    repayments = {}
    loan_pools = {
      'vip_loan': 0,
      'market_loan': 0
    }
    collateral = 0
  
    if balanceValue is None:
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
          balanceValue, repayments, max_loan = OKXHelper().get_balances(exch=exch)
          loan_pools['market_loan'] = OKXHelper().get_market_loan_pool(exch=exch)
          loan_pools['vip_loan'] = OKXHelper().get_VIP_loan_pool(exch=exch)

        elif exchange == "binance":
          if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
            balanceValue = Helper().get_pm_balances(exch=exch)
          else:
            balanceValue = Helper().get_balances(exch=exch)
            
        elif exchange == "bybit":
          balanceValue = BybitHelper().get_balances(exch=exch)

        elif exchange == "huobi":
          cm_balances = {}
          try:
            cm_balances = HuobiHelper().get_cm_balances(exch=exch)
          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " CM balances " + str(e))
              print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " CM balances " + str(e))
              logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)

              return True
          
          um_balances = {}
          try:
            um_balances = HuobiHelper().get_um_balances(exch=exch)
          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " UM balances " + str(e))
              print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " UM balances " + str(e))
              logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)

              return True

          future_balances = {}
          try:
            future_balances = HuobiHelper().get_future_balances(exch=exch)
          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " future balances " + str(e))
              print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " future balances " + str(e))
              logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)

              return True

          spot_balances = {}
          try:
            spot_balances = HuobiHelper().get_spot_balances(exch=exch)
          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " spot balances " + str(e))
              print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            else:
              logger.warning(client + " " + exchange + " " + sub_account + " spot balances " + str(e))
              logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)

              return True

          balanceValue = cm_balances
          for item in um_balances:
            if item in balanceValue:
              balanceValue[item] += um_balances[item]
            else:
              balanceValue[item] = um_balances[item]
          for item in future_balances:
            if item in balanceValue:
              balanceValue[item] += future_balances[item]
            else:
              balanceValue[item] = future_balances[item]
          for item in spot_balances:
            if item in balanceValue:
              balanceValue[item] += spot_balances[item]
            else:
              balanceValue[item] = spot_balances[item]

      except ccxt.ExchangeError as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " balances " + str(e))
          print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
        else:
          logger.warning(client + " " + exchange + " " + sub_account + " balances " + str(e))
          logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
        return True

      balanceValue = {_key: balanceValue[_key] for _key in balanceValue if balanceValue[_key] != 0.0}

      # calculate base balance
      
      query = {}
      if exchange:
        query["venue"] = exchange
      ticker_values = self.tickers_db.find(query)

      for item in ticker_values:
        ticker_value = item["ticker_value"]

      base_balance = 0
      if exchange == "binance":
        try:
          wallet_balances = Helper().get_wallet_balances(exch=exch)
          for item in wallet_balances:
            cross_ratio = Helper().calc_cross_ccy_ratio(
              "BTC", config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], ticker_value
            )

            if cross_ratio == 0:
              if logger == None:
                print(client + " " + exchange + " " + sub_account + " balances: skipped as zero ticker price")
                print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
              else:
                logger.error(client + " " + exchange + " " + sub_account + " balances: skipped as zero ticker price")
                logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
              return True
          
            base_balance += float(item['balance']) * cross_ratio
        except ccxt.ExchangeError as e:
          if logger == None:
            print(client + " " + exchange + " " + sub_account + " balances " + str(e))
          else:
            logger.warning(client + " " + exchange + " " + sub_account + " balances " + str(e))
          pass
      else:
        for _key, _value in balanceValue.items():
          cross_ratio = Helper().calc_cross_ccy_ratio(
            _key,
            config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"],
            ticker_value,
          )

          if cross_ratio == 0:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " balances: skipped as zero ticker price")
              print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            else:
              logger.error(client + " " + exchange + " " + sub_account + " balances: skipped as zero ticker price")
              logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            return True
          
          base_balance += _value * cross_ratio

      balanceValue["base"] = base_balance

      # calculate collateral

      if "collateral" in config["clients"][client]["subaccounts"][exchange][sub_account]:
        for _key, _value in config["clients"][client]["subaccounts"][exchange][sub_account]['collateral'].items():
          cross_ratio = Helper().calc_cross_ccy_ratio(
            _key,
            config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"],
            ticker_value
          )
          
          if cross_ratio == 0:
            if logger == None:
              print(client + " " + exchange + " " + sub_account + " balances: skipped as zero ticker price")
              print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            else:
              logger.error(client + " " + exchange + " " + sub_account + " balances: skipped as zero ticker price")
              logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
            return True
          
          collateral += _value * cross_ratio

    # calculate balance change
    
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

    latest_balance = 0
    latest_base_ccy = ""
    for item in balance_values:
      latest_balance = item['balance_value']['base']
      latest_base_ccy = item['base_ccy']

    balance_change = 0
    if latest_base_ccy == config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"]:
      balance_change = base_balance - latest_balance
      try:
        balance_change = balance_change / abs((latest_balance if latest_balance != 0.0 else base_balance))
      except:
        pass

    
    
    balance = {
      "client": client,
      "venue": exchange,
      "account": "Main Account",
      "balance_value": balanceValue,
      "repayments": repayments,
      "loan_pools": loan_pools,
      "balance_change": balance_change,
      "collateral": collateral,
      "base_ccy": config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"],
      "active": True,
      "entry": False,
      "exit": False,
      "timestamp": datetime.now(timezone.utc),
    }

    if sub_account:
      balance["account"] = sub_account
    if spot:
      balance["spotMarket"] = spot
    if future:
      balance["futureMarket"] = future
    if perp:
      balance["perpMarket"] = perp
    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass

    balance["runid"] = latest_run_id

    try:
      if config["balances"]["store_type"] == "timeseries":
        self.balances_db.insert_one(balance)
      elif config["balances"]["store_type"] == "snapshot":
        self.balances_db.update_one(
          {
            "client": balance["client"],
            "venue": balance["venue"],
            "account": balance["account"],
          },
          {
            "$set": {
              "balance_value": balance["balance_value"],
              "active": balance["active"],
              "entry": balance["entry"],
              "exit": balance["exit"],
              "timestamp": balance["timestamp"],
              "runid": balance["runid"],
            }
          },
          upsert=True,
        )

      if logger == None:
        print("Collected balances for " + client + " " + exchange + " " + sub_account)
      else:
        logger.info("Collected balances for " + client + " " + exchange + " " + sub_account)

      return True
    
    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + sub_account + " balances " + str(e))
        print("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
      else:
        logger.error(client + " " + exchange + " " + sub_account + " balances " + str(e))
        logger.error("Unable to collect balances for " + client + " " + exchange + " " + sub_account)
      return True

  