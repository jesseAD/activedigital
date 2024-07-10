from datetime import datetime, timezone, timedelta

from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

# pd.set_option('future.no_silent_downcasting', True)
config = read_config_file()

class FundingContributions():
  def __init__(self, db, collection):
    self.runs_db = db[config['mongodb']['database']]['runs']
    self.positions_db = db[config['mongodb']['database']]['positions']
    self.funding_rates_db = db[config['mongodb']['database']]['funding_rates']
    self.borrow_rates_db = db[config['mongodb']['database']]['borrow_rates']
    self.funding_contributions_db = db[config['mongodb']['database']][collection]

  def create(
    self,
    client,
    exch = None,
    exchange: str = None,
    account: str = None,
    logger=None,
    session=None,
    secrets={}
  ):
    try:
      prev_values = self.funding_contributions_db.aggregate([
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
      ])

      prev_value = None
      for item in prev_values:
        prev_value = item

      if prev_value == None:
        prev_positions = self.positions_db.aggregate([
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
        prev_position = None
        for item in prev_positions:
          prev_position = item

        if prev_position == None:
          if logger == None:
            print(client + " " + exchange + " " + account + " funding contributions: no position history")
            print("Unable to collect funding contributions for " + client + " " + exchange + " " + account)
          else:
            logger.warning(client + " " + exchange + " " + account + " funding contributions: no position history")
            logger.error("Unable to collect funding contributions for " + client + " " + exchange + " " + account)

          return True
        
        prev_value = {
          'timestamp': datetime(
              prev_position['timestamp'].year, 
              prev_position['timestamp'].month, 
              prev_position['timestamp'].day, 
              int(prev_position['timestamp'].hour / config['funding_contributions']['period'][exchange]) * config['funding_contributions']['period'][exchange]
            )
        }
        
      prev_time = prev_value['timestamp'].replace(tzinfo=timezone.utc)

      now = datetime.now(timezone.utc)
      base_time = datetime(
        now.year, now.month, now.day, 
        int(now.hour / config['funding_contributions']['period'][exchange]) * config['funding_contributions']['period'][exchange]
      ).replace(tzinfo=timezone.utc)

      if(prev_time >= base_time):
        if logger == None:
          print(client + " " + exchange + " " + account + " funding contributions: didn't reach the period")
          print("Unable to collect funding contributions for " + client + " " + exchange + " " + account)
        else:
          logger.warning(client + " " + exchange + " " + account + " funding contributions: didn't reach the period")
          logger.error("Unable to collect funding contributions for " + client + " " + exchange + " " + account)

        return True
      
      base_time = prev_time + timedelta(hours=config['funding_contributions']['period'][exchange])

      funding_contribution_values = []
      while(base_time <= now):
        positions = list(self.positions_db.aggregate([
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
            '$unwind': "$position_value"
          }, {
            '$group': {
              '_id': {'base': "$position_value.base", 'side': "$position_value.side"},
              'base': {'$last': "$position_value.base"},
              'side': {'$last': "$position_value.side"},
              'notional': {'$last': {'$toDouble': "$position_value.notional"}}
            }
          }
        ]))
        positions.sort(key = lambda x: x['base'])

        funding_rates = list(self.funding_rates_db.aggregate([
          {
            '$match': {
              '$expr': {
                '$and': [{
                    '$eq': ['$venue', exchange]
                  }, {
                    '$eq': ['$funding_rates_value.quote', "USDT"]
                  }, {
                    '$lte': ['$timestamp', base_time]
                  }, {
                    '$gt': ['$timestamp', prev_time]
                  }
                ]
              }
            }
          }, {
            '$group': {
              '_id': {'symbol': "$symbol"},
              'base': {'$last': "$funding_rates_value.base"},
              'rate': {'$avg': {'$multiply': ["$funding_rates_value.scalar", "$funding_rates_value.fundingRate"]}},
            }
          }
        ]))
        funding_rates = {
          item['base']: item['rate'] for item in funding_rates
        }

        total_notional = sum(abs(item['notional']) for item in positions)
        funding_contributions = [{
          'base': item['base'],
          'contribution': abs(item['notional']) / total_notional * funding_rates[item['base']] * (1 if item['side'] == "short" else -1)
        } for item in positions if item['base'] in funding_rates]

        if len(funding_contributions) > 0:
          funding_contribution_values.append({
            'funding_contributions': funding_contributions,
            'timestamp': base_time
          })

        prev_time = base_time
        base_time = base_time + timedelta(hours=config['funding_contributions']['period'][exchange])

    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + account + " funding contributions " + str(e))
        print("Unable to collect funding contributions for " + client + " " + exchange + " " + account)
      else:
        logger.error(client + " " + exchange + " " + account + " funding contributions " + str(e))
        logger.error("Unable to collect funding contributions for " + client + " " + exchange + " " + account)

      return True
    
    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass

    funding_contribution = [{
      'client': client,
      'venue': exchange,
      'account': account,
      'funding_contributions': item['funding_contributions'],
      'timestamp': item['timestamp'],
      'runid': latest_run_id
    } for item in funding_contribution_values]

    try:
      self.funding_contributions_db.insert_many(funding_contribution)

      if logger == None:
        print("Collected funding contributions for " + client + " " + exchange + " " + account)
      else:
        logger.info("Collected funding contributions for " + client + " " + exchange + " " + account)

      return True
    
    except Exception as e:
      if logger == None:
        print(client + " " + exchange + " " + account + " funding contributions " + str(e))
        print("Unable to collect funding contributions for " + client + " " + exchange + " " + account)
      else:
        logger.error(client + " " + exchange + " " + account + " funding contributions " + str(e))
        logger.error("Unable to collect funding contributions for " + client + " " + exchange + " " + account)

      return True
