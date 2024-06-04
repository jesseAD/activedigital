from datetime import datetime, timezone, timedelta
import ccxt
import time
from math import log

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

config = read_config_file()

class DailyReturns():
  def __init__(self, db, collection):
    self.runs_db = db[config['mongodb']['database']]['runs']
    self.balances_db = db[config['mongodb']['database']]['balances']
    self.transactions_db = db[config['mongodb']['database']]['transaction_union']
    self.daily_returns_db = db[config['mongodb']['database']][collection]

  def create(
    self,
    client,
    exch = None,
    exchange: str = None,
    account: str = None,
    logger=None,
    session=None,
    secrets={},
    balance_finished={}
  ):
    try:
      if session == None:
        while(not balance_finished[client + "_" + exchange + "_" + account]):
          if logger == None:
            print(client + " " + exchange + " " + account + " daily returns: balances was not finished")
          else:
            logger.info(client + " " + exchange + " " + account + " daily returns: balances was not finished")

          time.sleep(0.5)

      prev_returns = self.daily_returns_db.aggregate([
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
                    '$account', account
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
      ], session=session)
      prev_return = None
      for item in prev_returns:
        prev_return = item

      if prev_return == None:
        prev_return = {
          'timestamp': datetime.now(timezone.utc) - timedelta(hours=config['daily_returns']['period'])
        }

      now = datetime.now(timezone.utc)
      base_time = datetime(now.year, now.month, now.day, int(now.hour / config['daily_returns']['period']) * config['daily_returns']['period']).replace(tzinfo=timezone.utc)

      if(prev_return['timestamp'].replace(tzinfo=timezone.utc) >= base_time):
        if logger == None:
          print(client + " " + exchange + " " + account + " daily returns: didn't reach the period")
          print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
        else:
          logger.warning(client + " " + exchange + " " + account + " daily returns: didn't reach the period")
          logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)

        return True

      base_time = datetime(
        prev_return['timestamp'].year, 
        prev_return['timestamp'].month, 
        prev_return['timestamp'].day,
        int(prev_return['timestamp'].hour / config['daily_returns']['period']) * config['daily_returns']['period']
      ).replace(tzinfo=timezone.utc) + timedelta(hours=config['daily_returns']['period'])

      dailyReturnsValue = []

      while(base_time <= now):
        print(base_time)
        last_balances = self.balances_db.aggregate([
          {
            '$match': {
              '$expr': {
                '$and': [
                  {
                    '$eq': ['$client', client]
                  }, {
                    '$eq': ['$venue', exchange]
                  }, {
                    '$eq': ['$account', account]
                  }, {
                    '$lte': ['$timestamp', base_time]
                  }
                ]
              }
            }
          }, {
            '$sort': {'timestamp': -1}
          }, {
            '$limit': 1
          }
        ], session=session)
        last_balance = 1
        for item in last_balances:
          last_balance = item['balance_value']['base']

        prev_balances = self.balances_db.aggregate([
          {
            '$match': {
              '$expr': {
                '$and': [
                  {
                    '$eq': ['$client', client]
                  }, {
                    '$eq': ['$venue', exchange]
                  }, {
                    '$eq': ['$account', account]
                  }, {
                    '$lte': ['$timestamp', base_time - timedelta(hours=config['daily_returns']['period'])]
                  }
                ]
              }
            }
          }, {
            '$sort': {'timestamp': -1}
          }, {
            '$limit': 1
          }
        ], session=session)
        prev_balance = None
        for item in prev_balances:
          prev_balance = item

        transfers = self.transactions_db.find({
          '$and': [
            {'client': client},
            {'venue': exchange},
            {'account': account},
            {'$or': [
              {"incomeType": "COIN_SWAP_WITHDRAW"},
              {"incomeType": "COIN_SWAP_DEPOSIT"}
            ]},
            {'timestamp': {
              '$gte': int((base_time - timedelta(hours=config['daily_returns']['period'])).timestamp() * 1000) - config['transactions']['time_slack'],
              '$lt': int(base_time.timestamp() * 1000) - config['transactions']['time_slack']
            }}
          ]
        }, session=session)
        transfer = sum([item['income'] for item in transfers])

        if prev_balance == None:
          dailyReturnsValue.append({
            'return': 0,
            'timestamp': base_time
          })
        else:
          dailyReturnsValue.append({
            'return': log(last_balance - transfer) - log(prev_balance['balance_value']['base']),
            'timestamp': base_time
          })

        base_time = base_time + timedelta(hours=config['daily_returns']['period'])

    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + account + " daily returns " + str(e))
        print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
      else:
        logger.error(client + " " + exchange + " " + account + " daily returns " + str(e))
        logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)

      return True

    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass
    
    daily_return = [{
      'client': client,
      'venue': exchange,
      'account': account,
      'return': item['return'],
      'timestamp': item['timestamp'],
      'runid': latest_run_id
    } for item in dailyReturnsValue]

    try:
      self.daily_returns_db.insert_many(daily_return, session=session)

      if logger == None:
        print("Collected daily returns for " + client + " " + exchange + " " + account)
      else:
        logger.info("Collected daily returns for " + client + " " + exchange + " " + account)

      return True
    
    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + account + " daily returns " + str(e))
        print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
      else:
        logger.error(client + " " + exchange + " " + account + " daily returns " + str(e))
        logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)

      return True