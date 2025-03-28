from datetime import datetime, timezone, timedelta, date
import ccxt 
import time

from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper, DeribitHelper

config = read_config_file()


class Transactions:
  def __init__(self, db, collection):

    self.runs_db = db[config['mongodb']['database']]['runs']
    self.tickers_db = db[config['mongodb']['database']]['tickers']
    self.transactions_db = db[config['mongodb']['database']][collection]
    self.transactions_union_db = db[config['mongodb']['database']]['transactions_union']
    self.mtd_pnls_db = db[config['mongodb']['database']]['mtd_pnls']

  # def get(
  #   self,
  #   client,
  #   active: bool = None,
  #   spot: str = None,
  #   future: str = None,
  #   perp: str = None,
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
  #   if client:
  #     pipeline.append({"$match": {"client": client}})
  #   if exchange:
  #     pipeline.append({"$match": {"venue": exchange}})
  #   if account:
  #     pipeline.append({"$match": {"account": account}})

  #   try:
  #     results = self.transactions_db.aggregate(pipeline)
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
    transaction_value: str = None,
    symbol: str = None,
    logger=None,
    secrets={},
  ):
    if transaction_value is None:
      if exch == None:
        spec = client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_"
        API_KEY = secrets[spec + "API_KEY"]
        API_SECRET = secrets[spec + "API_SECRET"]
        PASSPHRASE = None
        if exchange == "okx":
          PASSPHRASE = secrets[spec + "PASSPHRASE"]

        exch = Exchange(exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE).exch()
      
      try:
        if config["transactions"]["store_type"] == "snapshot":
          if exchange == "okx":
            transactions = OKXHelper().get_transactions(exch=exch)
            for item in transactions:
              item['info'] = {**item}

            transaction_value = Mapping().mapping_transactions(
              exchange=exchange, transactions=transactions
            )

          elif exchange == "bybit":
            transactions = BybitHelper().get_transactions(
              exch=exch, params={'limit': 100}
            )
            for item in transactions:
              item['info'] = {**item}

            transactions_value = Mapping().mapping_transactions(
              exchange=exchange, transactions=transactions
            )

          elif exchange == "binance":
            if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
              cm_trades = Helper().get_cm_transactions(exch=exch, params={'limit': 100})
              for item in futures_trades:
                item['info'] = {**item}
              cm_trades = Mapping().mapping_transactions(
                exchange=exchange, transactions=cm_trades
              )
              cm_trades = Helper().get_um_transactions(exch=exch, params={'limit': 100})
              for item in futures_trades:
                item['info'] = {**item}
              um_trades = Mapping().mapping_transactions(
                exchange=exchange, transactions=cm_trades
              )
              transaction_value = {'cm': cm_trades, 'um': um_trades}
            else:
              futures_trades = Helper().get_future_transactions(
                exch=exch, params={"limit": 100}
              )
              for item in futures_trades:
                item['info'] = {**item}

              futures_trades = Mapping().mapping_transactions(
                exchange=exchange, transactions=futures_trades
              )
              spot_trades = Helper().get_spot_transactions(
                exch=exch, params={"symbol": symbol}
              )
              for item in spot_trades:
                item['info'] = {**item}

              spot_trades = Mapping().mapping_transactions(
                exchange=exchange, transactions=spot_trades
              )

              transaction_value = {"future": futures_trades, "spot": spot_trades}

        elif config["transactions"]["store_type"] == "timeseries":
          if exchange == "okx":
            transactions_values = self.transactions_union_db.aggregate([
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
                '$sort': {'transaction_value.timestamp': -1}
              }, {
                '$limit': 1
              }
            ])

            current_value = None
            for item in transactions_values:
              current_value = item['transaction_value']

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = OKXHelper().get_transactions(exch=exch, params={"begin": last_time, "end": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              if config["transactions"]["fetch_type"] == "id":
                last_id = int(current_value['info']["billId"]) + 1

                transactions = []

                while(True):
                  end_id = int(transactions[0]['billId']) - 1 if len(transactions) > 0 else int(1e30)

                  try:
                    res = OKXHelper().get_transactions(exch=exch, params={"after": end_id, "before": last_id})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['billId'])
                  transactions = res + transactions

                  time.sleep(0.5)

                for item in transactions:
                  item['info'] = {**item}
                transaction_value = Mapping().mapping_transactions(
                  exchange=exchange, transactions=transactions
                )
              elif config["transactions"]["fetch_type"] == "time":
                last_time = int(current_value["timestamp"]) + 1 + config['transactions']['time_slack']

                transactions = []

                while(True):
                  end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                  
                  try:
                    res = OKXHelper().get_transactions(exch=exch, params={"begin": last_time, "end": end_time})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['ts'])
                  transactions = res + transactions

                  time.sleep(0.5)

                for item in transactions:
                  item['info'] = {**item}
                transaction_value = Mapping().mapping_transactions(
                  exchange=exchange, transactions=transactions
                )

          elif exchange == "deribit":
            transactions_values = self.transactions_union_db.aggregate([
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
                '$sort': {'transaction_value.timestamp': -1}
              }, {
                '$limit': 1
              }
            ])

            current_value = None
            for item in transactions_values:
              current_value = item['transaction_value']

            if current_value is None:
              last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
              end_time = int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
              
              transaction_value = DeribitHelper().get_transactions(exch=exch, params={"start_timestamp": last_time, "end_timestamp": end_time})

              transaction_value.sort(key = lambda x: x['timestamp'])

              for item in transaction_value:
                item['info'] = {**item}

              transaction_value = Mapping().mapping_transactions(
                exchange=exchange, transactions=transaction_value
              )

            else:
              last_time = int(current_value["timestamp"]) + 1 + config['transactions']['time_slack']
              end_time = int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
              transaction_value = DeribitHelper().get_transactions(exch=exch, params={"start_timestamp": last_time, "end_timestamp": end_time})

              transaction_value.sort(key = lambda x: x['timestamp'])

              for item in transaction_value:
                item['info'] = {**item}
              transaction_value = Mapping().mapping_transactions(
                exchange=exchange, transactions=transaction_value
              )

          elif exchange == "binance":
            transaction_value = {}
            if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
              transactions_values = self.transactions_union_db.aggregate([
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
                  '$sort': {'transaction_value.timestamp': -1}
                }, {
                  '$limit': 1
                }
              ])

              current_value = None
              for item in transactions_values:
                current_value = item['transaction_value']

              if current_value is None:
                cm_trades = []

                while(True):
                  last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                  end_time = int(cm_trades[0]['time']) - 1 if len(cm_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_cm_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "limit": 1000})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  cm_trades = res + cm_trades

                  time.sleep(0.5)

                for item in cm_trades:
                  item['info'] = {**item}
                cm_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=cm_trades
                )

                transaction_value['cm'] = cm_trades

              else:
                last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                cm_trades = []

                while(True):
                  end_time = int(cm_trades[0]['time']) - 1 if len(cm_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_cm_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "limit": 1000})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  cm_trades = res + cm_trades

                  time.sleep(0.5)

                for item in cm_trades:
                  item['info'] = {**item}

                cm_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=cm_trades
                )

                transaction_value['cm'] = cm_trades

              if current_value is None:
                um_trades = []

                while(True):
                  last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                  end_time = int(um_trades[0]['time']) - 1 if len(um_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_um_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "limit": 1000})
                  except: 
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  um_trades = res + um_trades

                  time.sleep(0.5)

                for item in um_trades:
                  item['info'] = {**item}
                um_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=um_trades
                )

                transaction_value['um'] = um_trades

              else:
                last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                um_trades = []

                while(True):
                  end_time = int(um_trades[0]['time']) - 1 if len(um_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_um_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "limit": 1000})
                  except: 
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  um_trades = res + um_trades

                  time.sleep(0.5)

                for item in um_trades:
                  item['info'] = {**item}

                um_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=um_trades
                )

                transaction_value['um'] = um_trades

              if current_value is None:
                borrow_trades = []

                while(True):
                  last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                  end_time = int(borrow_trades[0]['interestAccuredTime']) - 1 if len(borrow_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_pm_borrow_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "size": 100})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['interestAccuredTime'])
                  borrow_trades = res + borrow_trades

                  time.sleep(0.5)

                for item in borrow_trades:
                  item['info'] = {**item}
                  item['interest'] = -float(item['interest'])
                  
                borrow_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=borrow_trades
                )

                transaction_value['borrow'] = borrow_trades
              else:
                last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                borrow_trades = []

                while(True):
                  end_time = int(borrow_trades[0]['interestAccuredTime']) - 1 if len(borrow_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_pm_borrow_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "size": 100})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['interestAccuredTime'])
                  borrow_trades = res + borrow_trades

                  time.sleep(0.5)
                
                borrow_trades = borrow_trades[1:]
                
                for item in borrow_trades:
                  item['info'] = {**item}
                  item['interest'] = -float(item['interest'])

                borrow_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=borrow_trades
                )

                transaction_value['borrow'] = borrow_trades

              if current_value is None:
                transfers = []

                while(True):
                  last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                  end_time = int(transfers[0]['time']) - 1 if len(transfers) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_transfers(exch=exch, params={"startTime": last_time, "endTime": end_time})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  transfers = res + transfers

                  time.sleep(0.5)

                for item in transfers:
                  item['info'] = {**item}
                  item['incomeType'] = "TRANSFER"
                  
                transfers = Mapping().mapping_transactions(
                  exchange=exchange, transactions=transfers
                )

                transaction_value['transfer'] = transfers

              else:
                last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                transfers = []

                while(True):
                  end_time = int(transfers[0]['time']) - 1 if len(transfers) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_transfers(exch=exch, params={"startTime": last_time, "endTime": end_time})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  transfers = res + transfers

                  time.sleep(0.5)
                
                for item in transfers:
                  item['info'] = {**item}
                  item['incomeType'] = "TRANSFER"

                transfers = Mapping().mapping_transactions(
                  exchange=exchange, transactions=transfers
                )

                transaction_value['transfer'] = transfers

            else:
              transactions_values = self.transactions_union_db.aggregate([
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
                  '$sort': {'transaction_value.timestamp': -1}
                }, {
                  '$limit': 1
                }
              ])

              current_value = None
              for item in transactions_values:
                current_value = item['transaction_value']

              if current_value is None:
                futures_trades = []

                while(True):
                  last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                  end_time = int(futures_trades[0]['time']) - 1 if len(futures_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_future_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "limit": 1000})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  futures_trades = res + futures_trades

                  time.sleep(0.5)

                for item in futures_trades:
                  item['info'] = {**item}
                futures_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=futures_trades
                )

                transaction_value["future"] = futures_trades
              else:
                last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                futures_trades = []

                while(True):
                  end_time = int(futures_trades[0]['time']) - 1 if len(futures_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_future_transactions(exch=exch, params={"startTime": last_time, "endTime": end_time, "limit": 1000})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  futures_trades = res + futures_trades

                  time.sleep(0.5)

                for item in futures_trades:
                  item['info'] = {**item}

                futures_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=futures_trades
                )

                transaction_value['future'] = futures_trades

              if current_value is None:
                spot_trades = []

                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                while(True):
                  end_time = int(spot_trades[0]['time']) - 1 if len(spot_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_spot_transactions(exch=exch, params={"startTime": max(last_time, end_time - 82800000), "endTime": end_time, "symbol": symbol, "limit": 1000})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  spot_trades = res + spot_trades

                  time.sleep(0.5)

                for item in spot_trades:
                  item['info'] = {**item}

                spot_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=spot_trades
                )

                transaction_value["spot"] = spot_trades

              else:
                if config["transactions"]["fetch_type"] == "id":
                  last_id = int(current_value["id"]) + 1
                  spot_trades = Helper().get_spot_transactions(
                    exch=exch,
                    params={
                      "fromId": last_id,
                      "symbol": symbol,
                      "limit": 1000,
                    },
                  )

                elif config["transactions"]["fetch_type"] == "time":
                  last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                  spot_trades = []

                  while(True):
                    end_time = int(spot_trades[0]['time']) - 1 if len(spot_trades) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                    try:
                      res = Helper().get_spot_transactions(exch=exch, params={"startTime": max(last_time, end_time - 82800000), "endTime": end_time, "symbol": symbol, "limit": 1000})
                    except:
                      break
                    
                    if len(res) == 0:
                      break

                    res.sort(key = lambda x: x['time'])
                    spot_trades = res + spot_trades

                    time.sleep(0.5)
                
                for item in spot_trades:
                  item['info'] = {**item}

                spot_trades = Mapping().mapping_transactions(
                  exchange=exchange, transactions=spot_trades
                )
                transaction_value["spot"] = spot_trades

              if current_value is None:
                transfers = []

                while(True):
                  last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                  end_time = int(transfers[0]['time']) - 1 if len(transfers) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_transfers(exch=exch, params={"startTime": last_time, "endTime": end_time})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  transfers = res + transfers

                  time.sleep(0.5)

                for item in transfers:
                  item['info'] = {**item}
                  item['incomeType'] = "TRANSFER"
                  
                transfers = Mapping().mapping_transactions(
                  exchange=exchange, transactions=transfers
                )

                transaction_value['transfer'] = transfers

              else:
                last_time = current_value["timestamp"] + 1 + config['transactions']['time_slack']

                transfers = []

                while(True):
                  end_time = int(transfers[0]['time']) - 1 if len(transfers) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

                  try:
                    res = Helper().get_transfers(exch=exch, params={"startTime": last_time, "endTime": end_time})
                  except:
                    break

                  if len(res) == 0:
                    break

                  res.sort(key = lambda x: x['time'])
                  transfers = res + transfers

                  time.sleep(0.5)
                
                for item in transfers:
                  item['info'] = {**item}
                  item['incomeType'] = "TRANSFER"

                transfers = Mapping().mapping_transactions(
                  exchange=exchange, transactions=transfers
                )

                transaction_value['transfer'] = transfers

          elif exchange == "bybit":
            transaction_value = {}

            transactions_values = self.transactions_union_db.aggregate([
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
                '$sort': {'transaction_value.timestamp': -1}
              }, {
                '$limit': 1
              }
            ])

            current_value = None
            for item in transactions_values:
              current_value = item['transaction_value']

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['transactionTime']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = BybitHelper().get_commissions(exch=exch, params={'startTime': max(last_time, end_time - 518400000), "endTime": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['transactionTime'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['commission'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['transactionTime']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = BybitHelper().get_commissions(exch=exch, params={'startTime': max(last_time, end_time - 518400000), "endTime": end_time})
                except:
                  break


                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['transactionTime'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['commission'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['createdTime']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = BybitHelper().get_borrow_history(exch=exch, params={'startTime': max(last_time, end_time - 2073600000), "endTime": end_time, 'limit': 50})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['createdTime'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}
                item['funding'] = 0
                item['borrowCost'] = -float(item['borrowCost'])

              transaction_value['borrow'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['createdTime']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = BybitHelper().get_borrow_history(exch=exch, params={'startTime': max(last_time, end_time - 2073600000), "endTime": end_time, 'limit': 50})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['createdTime'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}
                item['funding'] = 0
                item['borrowCost'] = -float(item['borrowCost'])

              transaction_value['borrow'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )

          elif exchange == "huobi":
            transaction_value = {}

            transactions_values = self.transactions_union_db.aggregate([
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
                '$sort': {'transaction_value.timestamp': -1}
              }, {
                '$limit': 1
              }
            ])

            current_value = None
            for item in transactions_values:
              current_value = item['transaction_value']

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_cross_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['cross'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_cross_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break


                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['cross'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_isolated_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['isolated'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_isolated_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break


                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['isolated'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_cm_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['cm'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_cm_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break


                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['cm'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_future_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['future'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_future_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break


                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['future'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )

            if current_value is None:
              transactions = []

              while(True):
                last_time = int(datetime.timestamp(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)) * 1000)
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_spot_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break

                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['spot'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
            else:
              last_time = int(current_value['timestamp']) + 1 + config['transactions']['time_slack']

              transactions = []

              while(True):
                end_time = int(transactions[0]['ts']) - 1 if len(transactions) > 0 else int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)
                
                try:
                  res = HuobiHelper().get_spot_transactions(exch=exch, params={'start_time': max(last_time, end_time - 169200000), "end_time": end_time})
                except:
                  break


                if len(res) == 0:
                  break

                res.sort(key = lambda x: x['ts'])
                transactions = res + transactions

                time.sleep(0.5)

              for item in transactions:
                item['info'] = {**item}

              transaction_value['spot'] = Mapping().mapping_transactions(
                exchange=exchange, transactions=transactions
              )
  
      except ccxt.ExchangeError as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " transactions " + str(e))
          print("Unable to collect transactions for " + client + " " + exchange + " " + sub_account)
        else:
          logger.error(client + " " + exchange + " " + sub_account + " transactions " + str(e))
          logger.error("Unable to collect transactions for " + client + " " + exchange + " " + sub_account)

        return True
    
    tickers = list(self.tickers_db.find({"venue": exchange}))[0]['ticker_value']
    
    current_time = datetime.now(timezone.utc)
    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass

    if config["transactions"]["store_type"] == "snapshot":
      transaction = {
        "client": client,
        "venue": exchange,
        "account": sub_account,
        "transaction_value": transaction_value,
        "runid": latest_run_id,
        "timestamp": current_time,
      }

    elif config["transactions"]["store_type"] == "timeseries":
      transaction = []

      if exchange == "okx":

        for item in transaction_value:
          item['timestamp'] = int(item["timestamp"]) - config['transactions']['time_slack']

          if item['fee'] != '':
            item['fee_origin'] = float(item['fee'])
            item['fee_base'] = (
              float(item["fee"]) * 
              Helper().calc_cross_ccy_ratio(
                item['ccy'],
                config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                tickers
              )
            )
            item['fee'] = float(item["fee"]) * Helper().calc_cross_ccy_ratio(item['ccy'], config['transactions']['convert_ccy'], tickers)
            
          if item['sz'] != '':
            item['sz_origin'] = float(item["sz"])
            item['sz_base'] = (
              float(item["sz"]) * 
              Helper().calc_cross_ccy_ratio(
                item['ccy'],
                config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                tickers
              )
            )
            item['sz'] = float(item["sz"]) * Helper().calc_cross_ccy_ratio(item['ccy'], config['transactions']['convert_ccy'], tickers)

          if item['pnl'] != '':
            item['pnl_origin'] = float(item['pnl'])
            item['pnl_base'] = (
              float(item["pnl"]) * 
              Helper().calc_cross_ccy_ratio(
                item['ccy'],
                config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                tickers
              )
            )
            item['pnl'] = float(item["pnl"]) * Helper().calc_cross_ccy_ratio(item['ccy'], config['transactions']['convert_ccy'], tickers)

          new_value = {
            "client": client,
            "venue": exchange,
            "account": sub_account,
            "transaction_value": item,
            "convert_ccy": config['transactions']['convert_ccy'],
            "runid": latest_run_id,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": current_time,
          }

          transaction.append(new_value)

      elif exchange == "binance":
        for _type in transaction_value:
          for item in transaction_value[_type]:
            item['timestamp'] = int(item["timestamp"]) - config['transactions']['time_slack']

            item['income_origin'] = float(item['income'])
            item['income_base'] = (
              float(item["income"]) * 
              Helper().calc_cross_ccy_ratio(
                item['asset'],
                config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                tickers
              )
            )
            item['income'] = float(item["income"]) * Helper().calc_cross_ccy_ratio(item['asset'], config['transactions']['convert_ccy'], tickers)

            new_value = {
              "client": client,
              "venue": exchange,
              "account": sub_account,
              "transaction_value": item,
              "trade_type": _type,
              "convert_ccy": config['transactions']['convert_ccy'],
              "runid": latest_run_id,
              "active": True,
              "entry": False,
              "exit": False,
              "timestamp": current_time,
            }

            transaction.append(new_value)   

      elif exchange == "deribit":
        for item in transaction_value:
          item['timestamp'] = int(item["timestamp"]) - config['transactions']['time_slack']

          item['change_origin'] = float(item['change'])
          item['change_base'] = (
            float(item["change"]) * 
            Helper().calc_cross_ccy_ratio(
              item['currency'],
              config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
              tickers
            )
          )
          item['change'] = float(item["change"]) * Helper().calc_cross_ccy_ratio(item['currency'], config['transactions']['convert_ccy'], tickers)

          new_value = {
            "client": client,
            "venue": exchange,
            "account": sub_account,
            "transaction_value": item,
            "convert_ccy": config['transactions']['convert_ccy'],
            "runid": latest_run_id,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": current_time,
          }

          transaction.append(new_value)   

      elif exchange == "huobi":
        for _type in transaction_value:
          for item in transaction_value[_type]:
            item['timestamp'] = int(item["timestamp"]) - config['transactions']['time_slack']

            item['amount_origin'] = float(item['amount'])
            item['amount_base'] = (
              float(item["amount"]) * 
              Helper().calc_cross_ccy_ratio(
                item['currency'],
                config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                tickers
              )
            )
            item['amount'] = float(item["amount"]) * Helper().calc_cross_ccy_ratio(item['currency'], config['transactions']['convert_ccy'], tickers)

            new_value = {
              "client": client,
              "venue": exchange,
              "account": sub_account,
              "transaction_value": item,
              "trade_type": _type,
              "convert_ccy": config['transactions']['convert_ccy'],
              "runid": latest_run_id,
              "timestamp": current_time,
            }

            transaction.append(new_value)   

      elif exchange == "bybit":
        for _type in transaction_value:
          for item in transaction_value[_type]:
            item['timestamp'] = int(item["timestamp"]) - config['transactions']['time_slack']

            if item['fee'] != '':
              item['fee_origin'] = float(item['fee'])
              item['fee_base'] = (
                float(item["fee"]) * 
                Helper().calc_cross_ccy_ratio(
                  item['currency'],
                  config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                  tickers
                )
              )
              item['fee'] = float(item['fee']) * Helper().calc_cross_ccy_ratio(item['currency'], config['transactions']['convert_ccy'], tickers)

            if item['funding'] != '':
              item['funding_origin'] = float(item['funding'])
              item['funding_base'] = (
                float(item["funding"]) * 
                Helper().calc_cross_ccy_ratio(
                  item['currency'],
                  config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                  tickers
                )
              )
              item['funding'] = float(item['funding']) * Helper().calc_cross_ccy_ratio(item['currency'], config['transactions']['convert_ccy'], tickers)

            if _type == "commission":
              item['fee'] = -item['fee']
              item['fee_origin'] = -item['fee_origin']
              item['fee_base'] = -item['fee_base']

              if item['cashFlow'] != '':
                item['cashFlow_origin'] = float(item['cashFlow'])
                item['cashFlow_base'] = (
                  float(item["cashFlow"]) * 
                  Helper().calc_cross_ccy_ratio(
                    item['currency'],
                    config["clients"][client]["subaccounts"][exchange][sub_account]["base_ccy"], 
                    tickers
                  )
                )
                item['cashFlow'] = float(item['cashFlow']) * Helper().calc_cross_ccy_ratio(item['currency'], config['transactions']['convert_ccy'], tickers)

            new_value = {
              "client": client,
              "venue": exchange,
              "account": sub_account,
              "transaction_value": item,
              "trade_type": _type,
              "convert_ccy": config['transactions']['convert_ccy'],
              "runid": latest_run_id,
              "timestamp": current_time,
            }

            transaction.append(new_value)

      # transactions_union

      transactions_union = []

      try:
        for item in transaction:
          income_type = ""

          if exchange == "binance":
            if item['transaction_value']['incomeType'] == "TRANSFER" and item['transaction_value']['income'] > 0:
              income_type = "COIN_SWAP_DEPOSIT"
            elif item['transaction_value']['incomeType'] == "TRANSFER" and item['transaction_value']['income'] < 0:
              income_type = "COIN_SWAP_WITHDRAW"
            if item['transaction_value']['incomeType'] == "INTERNAL_TRANSFER" and item['transaction_value']['income'] > 0:
              income_type = "COIN_SWAP_DEPOSIT"
            elif item['transaction_value']['incomeType'] == "INTERNAL_TRANSFER" and item['transaction_value']['income'] < 0:
              income_type = "COIN_SWAP_WITHDRAW"
            elif item['transaction_value']['incomeType'] == "COMMISSION":
              income_type = "COMMISSION"
            elif item['transaction_value']['incomeType'] == "COMMISSION_REBATE":
              income_type = "COMMISSION"
            elif item['transaction_value']['incomeType'] == "API_REBATE":
              income_type = "COMMISSION"
            elif item['transaction_value']['incomeType'] == "FUNDING_FEE":
              income_type = "FUNDING_FEE"
            elif item['transaction_value']['incomeType'] == "ON_BORROW":
              income_type = "BORROW"
            elif item['transaction_value']['incomeType'] == "PERIODIC":
              income_type = "BORROW"

            transactions_union.append({
              **item,
              'transaction_value': {
                'info': item['transaction_value']['info'],
                'symbol': item['transaction_value']['symbol'] if "symbol" in item['transaction_value'] else "",
                'asset': item['transaction_value']['asset'],
                'income': item['transaction_value']['income'],
                'income_base': item['transaction_value']['income_base'],
                'income_origin': item['transaction_value']['income_origin'],
                'timestamp': item['transaction_value']['timestamp'],
              },
              'incomeType': income_type
            })
        
          elif exchange == "okx":
            if item['transaction_value']['type'] == "2":
              income_type = "COMMISSION"
              income = item['transaction_value']['fee']
              income_base = item['transaction_value']['fee_base']
              income_origin = item['transaction_value']['fee_origin']
            elif item['transaction_value']['type'] == "8":
              income_type = "FUNDING_FEE"
              income = item['transaction_value']['pnl']
              income_base = item['transaction_value']['pnl_base']
              income_origin = item['transaction_value']['pnl_origin']
            elif item['transaction_value']['type'] == "15":
              income_type = "BORROW"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']
            elif item['transaction_value']['subType'] == "9":
              income_type = "BORROW"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']
            elif item['transaction_value']['subType'] == "11":
              income_type = "COIN_SWAP_DEPOSIT"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']
            elif item['transaction_value']['subType'] == "12":
              income_type = "COIN_SWAP_WITHDRAW"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']
            elif item['transaction_value']['subType'] == "173":
              income_type = "FUNDING_FEE"
              income = item['transaction_value']['pnl']
              income_base = item['transaction_value']['pnl_base']
              income_origin = item['transaction_value']['pnl_origin']
            elif item['transaction_value']['subType'] == "174":
              income_type = "FUNDING_FEE"
              income = item['transaction_value']['pnl']
              income_base = item['transaction_value']['pnl_base']
              income_origin = item['transaction_value']['pnl_origin']
            elif item['transaction_value']['subType'] == "210":
              income_type = "BORROW"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']
            elif item['transaction_value']['subType'] == "212":
              income_type = "BORROW"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']
            elif item['transaction_value']['subType'] == "17":
              income_type = "BORROW"
              income = item['transaction_value']['sz']
              income_base = item['transaction_value']['sz_base']
              income_origin = item['transaction_value']['sz_origin']

            transactions_union.append({
              **item,
              'transaction_value': {
                'info': item['transaction_value']['info'],
                'symbol': item['transaction_value']['instId'] if "instId" in item['transaction_value'] else "",
                'asset': item['transaction_value']['ccy'],
                'income': income,
                'income_base': income_base,
                'income_origin': income_origin,
                'timestamp': item['transaction_value']['timestamp'],
              },
              'incomeType': income_type
            })

          elif exchange == "bybit":
            income = item['transaction_value']['fee']
            income_base = item['transaction_value']['fee_base']
            income_origin = item['transaction_value']['fee_origin']

            if item['trade_type'] == "borrow":
              income_type = "BORROW"
            else:
              if item['transaction_value']['type'] == "TRADE":
                income_type = "COMMISSION"
              elif item['transaction_value']['type'] == "SETTLEMENT":
                income_type = "FUNDING_FEE"
                income = item['transaction_value']['funding']
                income_base = item['transaction_value']['funding_base']
                income_origin = item['transaction_value']['funding_origin']
              elif item['transaction_value']['type'] == "TRANSFER_IN":
                income_type = "COIN_SWAP_DEPOSIT"
                income = item['transaction_value']['cashFlow']
                income_base = item['transaction_value']['cashFlow_base']
                income_origin = item['transaction_value']['cashFlow_origin']
              elif item['transaction_value']['type'] == "TRANSFER_OUT":
                income_type = "COIN_SWAP_WITHDRAW"
                income = item['transaction_value']['cashFlow']
                income_base = item['transaction_value']['cashFlow_base']
                income_origin = item['transaction_value']['cashFlow_origin']

            transactions_union.append({
              **item,
              'transaction_value': {
                'info': item['transaction_value']['info'],
                'symbol': item['transaction_value']['symbol'] if "symbol" in item['transaction_value'] else "",
                'asset': item['transaction_value']['currency'],
                'income': income,
                'income_base': income_base,
                'income_origin': income_origin,
                'timestamp': item['transaction_value']['timestamp'],
              },
              'incomeType': income_type
            })

          elif exchange == "huobi":
            if item['transaction_value']['type'] == "30" or item['transaction_value']['type'] == "31":
              income_type = "FUNDING_FEE"
            elif (
              item['transaction_value']['type'] == "5" or 
              item['transaction_value']['type'] == "6" or 
              item['transaction_value']['type'] == "7" or 
              item['transaction_value']['type'] == "8" or
              item['trade_type'] == "spot"
            ):
              income_type = "COMMISSION"
            elif (
              item['transaction_value']['type'] == "14" or 
              item['transaction_value']['type'] == "34" or 
              item['transaction_value']['type'] == "37" or 
              item['transaction_value']['type'] == "38" 
            ):
              income_type = "COIN_SWAP_DEPOSIT"
            elif (
              item['transaction_value']['type'] == "15" or 
              item['transaction_value']['type'] == "36" or 
              item['transaction_value']['type'] == "39" 
            ):
              income_type = "COIN_SWAP_WITHDRAW"

            transactions_union.append({
              **item,
              'transaction_value': {
                'info': item['transaction_value']['info'],
                'symbol': item['transaction_value']['contract_code'] if "contract_code" in item['transaction_value'] else "",
                'asset': item['transaction_value']['currency'],
                'income': item['transaction_value']['amount'],
                'income_base': item['transaction_value']['amount_base'],
                'income_origin': item['transaction_value']['amount_origin'],
                'timestamp': item['transaction_value']['timestamp'],
              },
              'incomeType': income_type
            })

          elif exchange == "deribit":
            if (
              item['transaction_value']['type'] == "maker" or 
              item['transaction_value']['type'] == "taker" or 
              item['transaction_value']['type'] == "open" or 
              item['transaction_value']['type'] == "close" or
              item['transaction_value']['type'] == "liquidation" or
              item['transaction_value']['type'] == "buy" or
              item['transaction_value']['type'] == "sell" or
              item['transaction_value']['type'] == "delivery" or
              item['transaction_value']['type'] == "option" or
              item['transaction_value']['type'] == "future" or
              item['transaction_value']['type'] == "correction" or
              item['transaction_value']['type'] == "block_trade"
            ):
              income_type = "COMMISSION"
            elif item['transaction_value']['type'] == "deposit":
              income_type = "COIN_SWAP_DEPOSIT"
            elif item['transaction_value']['type'] == "withdrawal":
              income_type = "COIN_SWAP_WITHDRAW"

            transactions_union.append({
              **item,
              'transaction_value': {
                'info': item['transaction_value']['info'],
                'symbol': item['transaction_value']['instrument_name'] if "instrument_name" in item['transaction_value'] else "",
                'asset': item['transaction_value']['currency'],
                'income': item['transaction_value']['change'],
                'income_base': item['transaction_value']['change_base'],
                'income_origin': item['transaction_value']['change_origin'],
                'timestamp': item['transaction_value']['timestamp'],
              },
              'incomeType': income_type
            })

      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + sub_account + " transactions union " + str(e))
        else:
          logger.error(client + " " + exchange + " " + sub_account + " transactions union " + str(e))
    
    #  MTD PnL

    # try:
    #   pnl = 0

    #   if exchange == "binance":
    #     for _type in transaction_value:
    #       pnl += sum(float(item['income']) for item in transaction_value[_type])

    #   elif exchange == "okx":
    #     pnl = sum(float(item['sz']) for item in transaction_value if item['instType'] == "SPOT")

    #   elif exchange == "bybit":
    #     for _type in transaction_value:
    #       pnl += sum(float(item['funding']) for item in transaction_value[_type])

    #   last_pnls = self.mtd_pnls_db.aggregate([
    #     {
    #       '$match': {
    #         '$expr': {
    #           '$and': [
    #             {
    #               '$eq': [
    #                 '$client', client
    #               ]
    #             }, {
    #               '$eq': [
    #                 '$venue', exchange
    #               ]
    #             }, {
    #               '$eq': [
    #                 '$account', sub_account
    #               ]
    #             }
    #           ]
    #         }
    #       }
    #     }, {
    #       '$sort': {'date': -1}
    #     }, {
    #       '$limit': 1
    #     }
    #   ])

    #   last_pnl = None
    #   for item in last_pnls:
    #     last_pnl = item

    #   current_date = datetime.combine(date.today(), datetime.min.time())
      
    #   if last_pnl is None:
    #     self.mtd_pnls_db.insert_one({
    #       'client': client,
    #       'venue': exchange,
    #       'account': sub_account,
    #       'pnl': pnl,
    #       'cumulative_pnl': pnl,
    #       'date': current_date
    #     })
      
    #   else:
    #     if last_pnl['date'] == current_date:
    #       self.mtd_pnls_db.update_one(
    #         {
    #           'client': client,
    #           'venue': exchange,
    #           'account': sub_account,
    #           'date': current_date
    #         },
    #         {"$set": {
    #           'pnl': pnl + last_pnl['pnl'],
    #           'cumulative_pnl': pnl + last_pnl['cumulative_pnl'],
    #         }}
    #       )
        
    #     else:
    #       if current_date.day == 1:
    #         cumulative = 0
    #       else:
    #         cumulative = last_pnl['cumulative_pnl']

    #       self.mtd_pnls_db.insert_one({
    #         'client': client,
    #         'venue': exchange,
    #         'account': sub_account,
    #         'pnl': pnl,
    #         'cumulative_pnl': pnl + cumulative,
    #         'date': current_date
    #       })
    
    # except Exception as e:
    #   if logger == None:
    #     print(client + " " + exchange + " " + sub_account + " MTD PnL " + str(e))
    #   else:
    #     logger.warning(client + " " + exchange + " " + sub_account + " MTD PnL " + str(e))

    
    del transaction_value

    if len(transaction) <= 0:
      if logger == None:
        print(client + " " + exchange + " " + sub_account + " empty transactions")
        print("Unable to collect transactions for " + client + " " + exchange + " " + sub_account)
      else:
        logger.error(client + " " + exchange + " " + sub_account + " empty transactions")
        logger.error("Unable to collect transactions for " + client + " " + exchange + " " + sub_account)

      return True

    try:
      if config["transactions"]["store_type"] == "snapshot":
        self.transactions_union_db.update_one(
          {
            "client": transaction["client"],
            "venue": transaction["venue"],
            "account": transaction["account"],
          },
          {
            "$set": {
              "transaction_value": transaction["transaction_value"],
              "timestamp": transaction["timestamp"],
              "runid": transaction["runid"],
              "active": transaction["active"],
              "entry": transaction["entry"],
              "exit": transaction["exit"],
            }
          },
          upsert=True,
        )

      elif config["transactions"]["store_type"] == "timeseries":
        # self.transactions_db.insert_many(transaction)
        self.transactions_union_db.insert_many(transactions_union)

      del transaction

      if logger == None:
        print("Collected transactions for " + client + " " + exchange + " " + sub_account)
      else:
        logger.info("Collected transactions for " + client + " " + exchange + " " + sub_account)
      
      return True

    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + sub_account + " transactions " + str(e))
        print("Unable to collect transactions for " + client + " " + exchange + " " + sub_account)
      else:
        logger.error(client + " " + exchange + " " + sub_account + " transactions " + str(e))
        logger.error("Unable to collect transactions for " + client + " " + exchange + " " + sub_account)

      return True
