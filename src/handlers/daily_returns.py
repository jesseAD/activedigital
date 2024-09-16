from datetime import datetime, timezone, timedelta
import ccxt
import time
from math import log
import numpy as np
import pandas as pd

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

# pd.set_option('future.no_silent_downcasting', True)
config = read_config_file()

def cals_return(row):
  if row["prior_bals"] == 0:
    return 0
  else:
    if row["outlier"] == 1:
      #ln(row["balance_value"]-row["balance_change"]-row["prior_bals"])-ln(row["prior_bals"])
      # return (row["balance_value"]-row["balance_change"]-row["prior_bals"])/row["prior_bals"]
      return log(row["balance_value"]-row["balance_change"]+0.00001) - log(row["prior_bals"])
    else:
      #ln(row["balance_value"])-ln(row["prior_bals"])
      # return  (row["balance_value"]-row["prior_bals"])/row["prior_bals"]
      return  log(row["balance_value"]+0.00001) - log(row["prior_bals"])
    
def cal_ewma(data):
  decay=2/(config['daily_returns']['span']+1)
  #display(len(data[['balance_value','outlier']]))
  priorewma=0
  ewma=0
  outlier = 0
  # print(data[['balance_value','outlier']])
  for index, row in data[['balance_value','outlier']].iterrows():
    #display(row)
    if row['outlier'] == 0:
      if ewma == 0:
        ewma = row['balance_value']
      else:
        ewma=(row['balance_value']*decay)+((1-decay)*priorewma)
      priorewma=ewma
    else:
      outlier = 1
      ewma=row['balance_value']
      
  # return ewma
  return {'balance_value': ewma, 'outlier': outlier}

def get_ewmas(client_val,exchange_val,account_val,start_date, end_date, balances_db, transaction_union_db, base_ccy, ticker, collateral, session=None):
  pipeline =  [
  {"$match": {"$expr": {"$and": [
    {"$gte": ["$timestamp", {"$toDate":  int(start_date.timestamp()) * 1000}]},
    {"$lte": ["$timestamp", {"$toDate":  int(end_date.timestamp()) * 1000}]},
    {  "$eq": ["$client", client_val ] }  ,
    {  "$eq": ["$venue", exchange_val] },
    {  "$eq": ["$account", account_val] },
  ]}}},
  {"$project": {
    "_id":0,"client": "$client", "venue": "$venue", "account": "$account", "base_ccy":"$base_ccy", "balance_value": "$balance_value.base", "timestamp": 1, "collateral": 1
  }},
  {"$group": {
    "_id": {"client": "$client", "venue": "$venue", "account": "$account", "base_ccy": "$base_ccy","timestamp":"$timestamp"},  
    "client": {"$last": "$client"},
    "venue": {"$last": "$venue"},
    "account": {"$last": "$account"},
    "balance_value": {"$last": "$balance_value"},
    "base_ccy": {"$last": "$base_ccy"},
    "timestamp": {"$last": "$timestamp"},
    "collateral": {"$last": "$collateral"},
  }},
  {"$project": {"timestamp": 1, "_id": 0, "client": "$client", "venue": "$venue", "account": "$account","base":"$base_ccy", "balance_value": {"$ifNull": ["$balance_value", 0]}, "collateral": {"$ifNull": ["$collateral", 0]}}},
  ]
  balances=list(balances_db.aggregate(pipeline, session=session))
  for balance in balances:
    if balance['base'] != base_ccy:
      balance['balance_value'] = balance['balance_value'] * ticker - (balance['collateral'] * ticker - collateral)
    else:
      balance['balance_value'] -= (balance['collateral'] - collateral)

  balances_df = pd.json_normalize(balances)
  if balances_df.empty:
    dictionary={"timestamp":[datetime.now()], "client":[client_val], "venue":[exchange_val], "account":[account_val],"base":["USDT"],"balance_value":[0]}
    balances_df=pd.DataFrame(dictionary)

  balances_df.set_index('timestamp',inplace=True)
  balances_df.sort_index(inplace=True)
  #display(balances_df.head())
  transfers = list(transaction_union_db.find({
    "$and": [
      {"transaction_value.timestamp" : {"$gte":int(start_date.timestamp()) * 1000, "$lt":int(end_date.timestamp()) * 1000}},
      {
        "$or": [
          {"incomeType": "COIN_SWAP_WITHDRAW"}, 
          {"incomeType": "COIN_SWAP_DEPOSIT"}
        ]
      },
      {"client":client_val}
      ,
      {"venue":exchange_val}
      ,
      {"account":account_val}
    ]                                                                       
  }, session=session))
  transfers = [
    {
      **item,
      'timestamp': item['transaction_value']['timestamp']
    } for item in transfers
  ]
  transfers_df = pd.json_normalize(transfers)
  if transfers_df.empty:
      transfers_df=pd.DataFrame( columns=["_id","client","venue","account","convert_ccy","incomeType","symbol","trade_type","income","asset","timestamp","billId","ordId"])

  transfers_df=transfers_df.sort_values(by='timestamp')
  transfers_df['datetime'] = pd.to_datetime(transfers_df['timestamp'], unit='ms')
  transfers_df.set_index('datetime',inplace=True) 
  transfers_df.sort_index(inplace=True)

  balances_all = pd.merge_asof(balances_df,transfers_df, left_index = True, right_index = True,  allow_exact_matches=True,tolerance=pd.Timedelta("240Min"), suffixes=('bals', 'trans'),direction='backward')

  balances_all['income'].fillna(0, inplace=True)
  balances_all['balance_change']=balances_all['balance_value']-balances_all['balance_value'].shift(1)
  balances_all['balance_change'].fillna(0, inplace=True)
  Q1 = balances_all[balances_all['incomeType'].isnull()]['balance_change'].quantile(0.25)
  Q3 = balances_all[balances_all['incomeType'].isnull()]['balance_change'].quantile(0.75)
  IQR = Q3 - Q1
  lower = Q1 - 1.5*IQR
  upper = Q3 + 1.5*IQR
  balances_all['outlier']=np.where((balances_all['balance_change'] > upper) | (balances_all['balance_change'] < lower),1,0)

  adj_upper_array = np.where((balances_all['outlier'] >= 1) & (balances_all['income']==0 ))[0]
  balances_all.drop(index=balances_all.iloc[adj_upper_array].index, inplace=True)
  balances_all['balance_change']=balances_all['balance_value']-balances_all['balance_value'].shift(1)

  balances_all['prior_bals']=balances_all["balance_value"].shift(1)
  balances_1d=balances_all.resample(str(config['daily_returns']['resampling']) + 'h').apply(cal_ewma).to_frame()
  balances_1d = balances_1d.rename(columns= {0: 'ewma'})

  return balances_1d

class DailyReturns():
  def __init__(self, db, collection):
    self.runs_db = db[config['mongodb']['database']]['runs']
    self.balances_db = db[config['mongodb']['database']]['balances']
    self.leverages_db = db[config['mongodb']['database']]['leverages']
    self.tickers_db = db[config['mongodb']['database']]['tickers']
    self.transactions_db = db[config['mongodb']['database']]['transactions_union']
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
          'start_balance': 0,
          'end_balance': 0,
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
          prev_return = {
            'return': 0,
            'log_return': 0,
            'timestamp': base_time,
            'start_balance': last_balance['balance_value']['base'],
            'end_balance': last_balance['balance_value']['base'],
            'avg_leverage': 1,
            'transfer': 0,
            'base_ccy': last_balance['base_ccy']
          }
          dailyReturnsValue.append(prev_return)
          prev_time = prev_time + timedelta(hours=config['daily_returns']['period'])
          base_time = base_time + timedelta(hours=config['daily_returns']['period'])

        else:
          ticker = 1
          if last_balance['base_ccy'] != prev_balance['base_ccy']:
            tickers = self.tickers_db.find({'venue': exchange})
            for item in tickers:
              ticker_value = item['ticker_value']

            ticker = Helper().calc_cross_ccy_ratio(last_balance['base_ccy'], prev_balance['base_ccy'], ticker_value)

            if ticker == 0:
              if logger == None:
                print(client + " " + exchange + " " + account + " daily returns: skipped as zero ticker price")
                print("Unable to collect daily returns for " + client + " " + exchange + " " + account)
              else:
                logger.error(client + " " + exchange + " " + account + " daily returns: skipped as zero ticker price")
                logger.error("Unable to collect daily returns for " + client + " " + exchange + " " + account)
              return True
            
          try:
            transfers = list(self.transactions_db.find({
              '$and': [
                {'client': client},
                {'venue': exchange},
                {'account': account},
                {'$or': [
                  {"incomeType": "COIN_SWAP_WITHDRAW"},
                  {"incomeType": "COIN_SWAP_DEPOSIT"}
                ]},
                {'transaction_value.timestamp': {
                  '$gte': int(prev_time.timestamp() * 1000),
                  '$lt': int(base_time.timestamp() * 1000)
                }}
              ]
            }, session=session))
            transfer = sum([item['income_base'] for item in transfers])
            if transfer == 0.0:
              transfer = sum([item['income'] for item in transfers])

            leverages = list(self.leverages_db.find({
              '$and': [
                {'client': client},
                {'venue': exchange},
                {'account': account},
                {'timestamp': {
                  '$gte': prev_time,
                  '$lt': base_time
                }}
              ]
            }, session=session))
            avg_leverage = sum(item['leverage'] for item in leverages) / len(leverages)
            if avg_leverage == 0.0:
              avg_leverage = 1

            # collateral = (last_balance['collateral'] if 'collateral' in last_balance else 0) - (prev_balance['collateral'] if 'collateral' in prev_balance else 0)

            ewmas = get_ewmas(
              client, exchange, account, 
              prev_time - timedelta(hours=config['daily_returns']['overlap']), 
              base_time, self.balances_db, self.transactions_db, 
              prev_balance['base_ccy'],
              ticker,
              prev_balance['collateral'] if 'collateral' in prev_balance else 0,
              session
            ).to_dict(orient='records')

            start_balance = float(prev_return['end_balance'] if prev_return['end_balance'] > 0 else ewmas[0]['ewma']['balance_value'])
            end_balance = max(0.000000001, ewmas[-1]['ewma']['balance_value'] - transfer)
            outlier = sum([item['ewma']['outlier'] for item in ewmas])
            if outlier > 0:
              ret = 0
              log_ret = 0
              start_balance = end_balance
            else:
              log_ret = log(end_balance) - log(start_balance)
              ret = (end_balance - start_balance) / start_balance

            prev_return = {
              'return': ret,
              'log_return': log_ret,
              'timestamp': base_time,
              'start_balance': start_balance,
              'end_balance': ewmas[-1]['ewma']['balance_value'] / ticker,
              'avg_leverage': avg_leverage,
              'transfer': transfer,
              'base_ccy': last_balance['base_ccy']
            }
            dailyReturnsValue.append(prev_return)
          
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

    # get vip level
    
    vip_level = ""

    if session == None:
      try:
        if exchange == "deribit": 
          vip_level = config["clients"][client]["subaccounts"][exchange][account]["vip_level"]

        elif exchange == "huobi":
          vip_level = config["clients"][client]["subaccounts"][exchange][account]["vip_level"]

        elif exchange == "okx":
          vip_level = OKXHelper().get_vip_level(exch)

        elif exchange == "binance":
          vip_level = Helper().get_vip_level(exch)

        elif exchange == "bybit":
          vip_level = BybitHelper().get_vip_level(exch)

      except Exception as e:
        if logger == None:
          print(client + " " + exchange + " " + account + " vip level: " + str(e))
          print("Unable to collect balances for " + client + " " + exchange + " " + account)
        else:
          logger.error(client + " " + exchange + " " + account + " vip level: " + str(e))
          logger.error("Unable to collect balances for " + client + " " + exchange + " " + account)
    
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
      'tier': vip_level,
      'return': item['return'],
      'log_return': item['log_return'],
      'start_balance': item['start_balance'],
      'end_balance': item['end_balance'],
      'avg_leverage': item['avg_leverage'],
      'transfer': item['transfer'],
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