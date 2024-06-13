from datetime import datetime, timezone, timedelta
import ccxt
import time
from math import log
import numpy as np

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

config = read_config_file()

class DailyReturns():
  def __init__(self, db, collection):
    self.runs_db = db[config['mongodb']['database']]['runs']
    self.balances_db = db[config['mongodb']['database']]['balances']
    self.tickers_db = db[config['mongodb']['database']]['tickers']
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
    if session == None:
      if config['clients'][client]['subaccounts'][exchange][account]['daily_returns'] == False:
        return True
    
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
                  }
                ]
              }
            }
          }, {
            '$sort': {'timestamp': 1}
          }, {
            '$limit': 1
          }
        ], session=session)
        prev_balance = None
        for item in prev_balances:
          prev_balance = item

        if prev_balance == None:
          if logger == None:
            print(client + " " + exchange + " " + account + " daily returns: no balance history")
            print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
          else:
            logger.warning(client + " " + exchange + " " + account + " daily returns: no balance history")
            logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)

          return True
        
        prev_return = {
          'return': 0,
          'prev_balance': 0,
          'timestamp': prev_balance['timestamp']
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

      prev_time = datetime(
        prev_return['timestamp'].year, 
        prev_return['timestamp'].month, 
        prev_return['timestamp'].day,
        int(prev_return['timestamp'].hour / config['daily_returns']['period']) * config['daily_returns']['period']
      ).replace(tzinfo=timezone.utc)
      base_time = prev_time + timedelta(hours=config['daily_returns']['period'])

      dailyReturnsValue = []

      while(base_time <= now):
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
                  }, {
                    '$gt': ['$timestamp', prev_time]
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
        last_balance = None
        for item in last_balances:
          last_balance = item

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
                    '$lte': ['$timestamp', prev_time]
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

        

        if last_balance == None:
          base_time = base_time + timedelta(hours=config['daily_returns']['period'])
          
        elif prev_balance == None:
          dailyReturnsValue.append({
            'return': 0,
            'cum_return': 0,
            'timestamp': base_time,
            'prev_balance': last_balance['balance_value']['base'],
            'base_ccy': last_balance['base_ccy']
          })
          prev_time = prev_time + timedelta(hours=config['daily_returns']['period'])
          base_time = base_time + timedelta(hours=config['daily_returns']['period'])

        else:
          ticker = 1
          if last_balance['base_ccy'] != prev_balance['base_ccy']:
            tickers = self.tickers_db.find({'venue': exchange})
            for item in tickers:
              ticker_value = item['ticker_value']

            ticker = Helper().calc_cross_ccy_ratio(prev_balance['base_ccy'], last_balance['base_ccy'], ticker_value)

            if ticker == 0:
              if logger == None:
                print(client + " " + exchange + " " + account + " daily returns: skipped as zero ticker price")
                print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
              else:
                logger.error(client + " " + exchange + " " + account + " daily returns: skipped as zero ticker price")
                logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)
              return True
            
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
                '$gte': int(prev_time.timestamp() * 1000) - config['transactions']['time_slack'],
                '$lt': int(base_time.timestamp() * 1000) - config['transactions']['time_slack']
              }}
            ]
          }, session=session)

          transfers = [{
            **item,
            'timestamp': datetime.fromtimestamp((item['timestamp'] + config['transactions']['time_slack']) / 1000).replace(tzinfo=timezone.utc)
          } for item in transfers]
          # transfer = sum([item['income_base'] for item in transfers])

          balances = list(self.balances_db.find({
            'client': client,
            'venue': exchange,
            'account': account,
            'timestamp': {'$gt': prev_time, '$lte': base_time}
          }, session=session))

          for transfer in transfers:
            for balance in balances:
              if balance['timestamp'].replace(tzinfo=timezone.utc) >= transfer['timestamp']:
                balance['balance_value']['base'] = balance['balance_value']['base'] - transfer['income_base']

          for i in range(len(balances)):
            if i == 0:
              balances[i]['balance_change'] = balances[i]['balance_value']['base'] - prev_balance['balance_value']['base']
            else:
              balances[i]['balance_change'] = balances[i]['balance_value']['base'] - balances[i - 1]['balance_value']['base']

          Q1 = np.quantile([item['balance_change'] for item in balances], 0.25)
          Q3 = np.quantile([item['balance_change'] for item in balances], 0.75)
          IQR = Q3 - Q1
          lower = Q1 - 1.5 * IQR
          upper = Q3 + 1.5 * IQR
          for balance in balances:
            balance['outlier'] = (balance['balance_change'] > upper or balance['balance_change'] < lower)

          decay = 2 / ((len(balances) / 8) + 1)
          priorewma = 0
          ewma = 0
          for balance in balances:
            if balance['outlier']:
              if ewma == 0:
                ewma = balance['balance_value']['base']
              break

            else:
              ewma = (balance['balance_value']['base'] * decay) + ((1 - decay) * priorewma)
              priorewma = ewma
          
          balances[-1]['balance_value']['base'] = ewma

          # if balances[-1]['outlier']:
          #   for balance in balances:
          #     if not balance['outlier']:
          #       balances[-1]['balance_value']['base'] = balance['balance_value']['base']
            # balances[-1]['balance_value']['base'] = balances[-1]['balance_value']['base'] - balances[-1]['balance_change'] + 0.00001
          
          try:
            collateral = (balances[-1]['collateral'] if 'collateral' in balances[-1] else 0) - (prev_balance['collateral'] if 'collateral' in prev_balance else 0)
            ret = log(balances[-1]['balance_value']['base'] - collateral) - log(ticker * prev_balance['balance_value']['base'])
            dailyReturnsValue.append({
              'return': ret,
              # 'cum_return': ret + prev_return['cum_return'],
              'timestamp': base_time,
              'prev_balance': balances[-1]['balance_value']['base'],
              'base_ccy': balances[-1]['base_ccy']
            })
            # prev_return['cum_return'] = dailyReturnsValue[-1]['cum_return']

          except Exception as e:
            if logger == None:
              print(client + " " + exchange + " " + account + " daily returns " + str(e))
            else:
              logger.warning(client + " " + exchange + " " + account + " daily returns " + str(e))

          prev_time = base_time
          base_time = base_time + timedelta(hours=config['daily_returns']['period'])

    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + account + " daily returns " + str(e))
        print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
      else:
        logger.error(client + " " + exchange + " " + account + " daily returns " + str(e))
        logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)

      return True

    # print(dailyReturnsValue)
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
      # 'cum_return': item['cum_return'],
      'prev_balance': item['prev_balance'],
      'base_ccy': item['base_ccy'],
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