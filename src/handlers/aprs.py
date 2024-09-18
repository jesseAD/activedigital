import csv
from datetime import datetime, timezone
import concurrent.futures
import threading
import time
import ccxt 
from dotenv import load_dotenv

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper
from src.lib.apr_pairs import filter_insts, make_pairs

config = read_config_file()
condition = threading.Condition()
start_processing = False

def get_tickers(exch, exchange, tickers, param):
  with condition:
    while not start_processing:
      condition.wait()

    _tickers = {}

    try:
      if exchange == "binance":
        _tickers = exch.fetch_bids_asks(params={**param})
        for item in _tickers.keys():
          _tickers[item]['timestamp'] = int(_tickers[item]['info']['time']) if 'time' in _tickers[item]['info'] else int(datetime.now(timezone.utc).timestamp() * 1000)
        
      else:
        _tickers = exch.fetch_tickers(params={**param})

    except Exception as e:
      print("future opportunities " + str(e))

    tickers[exchange].update(_tickers)

def start_tickers():
  global start_processing

  with condition:
    start_processing = True
    condition.notify_all()

class Aprs:
  def __init__(self, db, collection):

    self.runs_db = db['runs']
    self.instruments_db = db['instruments']
    self.aprs_db = db[collection]

  
  
  def create(
    self,
    apr_value: str = None,
    logger=None
  ):
    prev_values = self.aprs_db.find({}).sort("_id", -1).limit(1)

    prev_value = None
    for item in prev_values:
      prev_value = item

    if prev_value != None:
      now = datetime.now(timezone.utc)
      base_time = datetime(
        now.year, now.month, now.day, 
        int(now.hour / config['future_opportunities']['period']) * config['future_opportunities']['period']
      ).replace(tzinfo=timezone.utc)

      prev_time = prev_value['timestamp'].replace(tzinfo=timezone.utc)

      if(prev_time >= base_time):
        if logger == None:
          print("Future opportunities: didn't reach the period")
          print("Unable to collect future opportunities")
        else:
          logger.warning("Future opportunities: didn't reach the period")
          logger.error("Unable to collect future opportunities")

        return True

    tickers = {exchange: {} for exchange in config['exchanges']}

    params = {
      'okx': [
        {'type': "spot"},
        {'type': "future"},
        {'type': "swap"},
      ],
      'binance': [
        {'type': "spot"},
        {'type': "swap", 'subType': "linear"},
        {'type': "swap", 'subType': "inverse"},
      ],
      'deribit': [
        {'currency': "BTC"},
        {'currency': "ETH"},
        {'currency': "USDT"},
        {'currency': "USDC"},
      ],
      
    }

    exchs = {exchange: [Exchange(exchange).exch() for item in params[exchange]] for exchange in params}

    threads = []
    # executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(config['exchanges']) * 5)

    # start_time = time.time()
    for exchange in params:
      for i in range(len(params[exchange])):
        thread = threading.Thread(target=get_tickers, args=(exchs[exchange][i], exchange, tickers, params[exchange][i], ))
        threads.append(thread)
        thread.start()
        # threads.append(executor.submit(get_tickers, exchs[exchange][i], exchange, tickers, params[exchange][i]))

    signal_thread = threading.Thread(target=start_tickers)
    signal_thread.start()

    for thread in threads:
      thread.join()

    signal_thread.join()

    # for thread in concurrent.futures.as_completed(threads):
    #   thread.cancel()

    # end_time = time.time()
    # print(end_time - start_time)

    instruments = self.instruments_db.find({'venue': {'$in': list(params.keys())}})
    instruments = {item['venue']: item['instrument_value'] for item in instruments}

    instruments = filter_insts(instruments)
    pairs = make_pairs(instruments)

    for pair in pairs:
      leg1_bid = tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['bid']
      leg1_ask = tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['ask']
      leg2_bid = tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['bid']
      leg2_ask = tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['ask']

      annualize = max(config['future_opportunities']['anualize'][pair['leg1']['exchange']], config['future_opportunities']['anualize'][pair['leg2']['exchange']])
      annualize /= (pair['leg2']['expiry'] - pair['leg1']['expiry'])

      pair['maker_apr'] = (leg2_ask - leg1_bid) / leg1_bid * annualize
      pair['mid_apr'] = (leg2_ask + leg2_bid - leg1_bid - leg1_ask) / (leg1_bid + leg1_ask) * annualize
      pair['taker_apr'] = (leg2_bid - leg1_ask) / leg1_ask * annualize
      pair['leg1']['bid'] = leg1_bid
      pair['leg1']['ask'] = leg1_ask
      pair['leg2']['bid'] = leg2_bid
      pair['leg2']['ask'] = leg2_ask

      pair['midpoint_spread'] = (leg2_bid + leg2_ask) / 2 - (leg1_ask + leg1_bid) / 2

      pair['leg1']['timestamp'] = tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['timestamp']
      pair['leg2']['timestamp'] = tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['timestamp']

      if pair['leg1']['exchange'] == "deribit":
        pair['leg1']['volume'] = float(tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['info']['volume_usd'])
        pair['leg1']['open_interest'] = float(tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['info']['open_interest']) if 'open_interest' in tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['info'] else 0
      
      elif pair['leg1']['exchange'] == "okx":
        pair['leg1']['volume'] = float(tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['baseVolume'])
        pair['leg1']['open_interest'] = float(tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['info']['open_interest']) if 'open_interest' in tickers[pair['leg1']['exchange']][pair['leg1']['symbol']]['info'] else 0

      if pair['leg2']['exchange'] == "deribit":
        pair['leg2']['volume'] = float(tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['info']['volume_usd'])
        pair['leg2']['open_interest'] = float(tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['info']['open_interest']) if 'open_interest' in tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['info'] else 0
      
      elif pair['leg2']['exchange'] == "okx":
        pair['leg2']['volume'] = float(tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['baseVolume'])
        pair['leg2']['open_interest'] = float(tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['info']['open_interest']) if 'open_interest' in tickers[pair['leg2']['exchange']][pair['leg2']['symbol']]['info'] else 0

    run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

    latest_run_id = 0
    for item in run_ids:
      try:
        latest_run_id = item["runid"]
      except:
        pass

    new_value = {
      "apr_values": pairs, 
      "timestamp": datetime.now(timezone.utc),
      "runid": latest_run_id,
    }

    try:
      self.aprs_db.insert_one(new_value)

      if logger == None:
        print("Collected future opportunities")
      else:
        logger.info("Collected future opportunities")

      return True
    
    except Exception as e:
      if logger == None:
        print("future opportunities " + str(e))
        print("Unable to collect future opportunities")
      else:
        logger.error("future opportunities " + str(e))
        logger.error("Unable to collect future opportunities")

      return True