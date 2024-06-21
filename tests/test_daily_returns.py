import json
import os, sys
import unittest
from unittest import mock
from math import log
import pytest
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
from dateutil import tz, relativedelta

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.handlers.daily_returns import DailyReturns
from src.handlers.helpers import Helper
from src.config import read_config_file
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture
def db_session():
  mongo_uri = None

  if os.getenv("mode") == "prod":
    mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
  db = MongoClient(mongo_uri)

  session = db.start_session()
  session.start_transaction()

  try:
    yield session
  finally:
    session.abort_transaction()


config = read_config_file('tests/config.yaml')

def test_ZeroReturnCalculatedWhenBalancesLessThanIntervalPeriod(db_session):
  db_session = db_session
  now = datetime.now(timezone.utc)
  base_time = datetime(now.year, now.month, now.day, int(now.hour / config['daily_returns']['period']) * config['daily_returns']['period']).replace(tzinfo=timezone.utc)
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'runid': 0,
      'timestamp': base_time
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )
  assert(
    len(list(db_session.client.active_digital.daily_returns.find(
      {'client': "vadym"}, 
      session=db_session)
    )) == 1
  )

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_SingleRetrunCalculatedWhenBalancesGenerated24HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 8.5,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=49)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 2)
  assert(round(returns[1]['return'], 5) == 0.16252)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 8.5)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_TwoRetrunCalculatedWhenBalancesWithWithdrawlGenerated48HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  now = datetime.now(timezone.utc)
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': now - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 13},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': now - timedelta(hours=72)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': now - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': now - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_WITHDRAW",
      'income_base': -1,
      'income': -1,
      'timestamp': int((now - timedelta(hours=24)).timestamp() * 1000)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 3)
  assert(round(returns[1]['return'], 5) == 0.10536)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[2]['return'], 5) == 0.18232)
  assert(round(returns[2]['end_balance'], 5) == 11)
  assert(round(returns[2]['start_balance'], 5) == 10)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[2]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_ThreeRetrunCalculatedWhenBalancesWithDepositlGenerated72HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=72)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 12},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=96)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=72)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_DEPOSIT",
      'income_base': 1,
      'income': 1,
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 4)
  assert(round(returns[1]['return'], 5) == 0.10536)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[2]['return'], 5) == 0.09531)
  assert(round(returns[2]['end_balance'], 5) == 11)
  assert(round(returns[2]['start_balance'], 5) == 10)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
  assert(returns[2]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[3]['return'], 5) == -0.20067)
  assert(round(returns[3]['end_balance'], 5) == 10)
  assert(round(returns[3]['start_balance'], 5) == 11)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[3]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_TwoRetrunCalculatedWhenBalancesWithCollateralCorrectionGenerated48HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=50)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=73)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=49)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 2.1,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 3)
  assert(round(returns[1]['return'], 5) == 0.10536)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[2]['return'], 5) == 0.09531)
  assert(round(returns[2]['end_balance'], 5) == 11)
  assert(round(returns[2]['start_balance'], 5) == 10)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[2]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_ThreeRetrunCalculatedWhenBalancesWithDepositWithdrawlAndWithCollateralCorrectionlGenerated72HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=72)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=96)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10.5},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=72)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.5,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_DEPOSIT",
      'income_base': 1,
      'income': 1,
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=25)).timestamp() * 1000)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_WITHDRAW",
      'income_base': -2.1,
      'income': -2.1,
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=49)).timestamp() * 1000)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 4)
  assert(round(returns[1]['return'], 5) == 0.15415)
  assert(round(returns[1]['end_balance'], 5) == 10.5)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[2]['return'], 5) == 0.22124)
  assert(round(returns[2]['end_balance'], 5) == 11)
  assert(round(returns[2]['start_balance'], 5) == 10.5)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
  assert(returns[2]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[3]['return'], 5) == -0.20067)
  assert(round(returns[3]['end_balance'], 5) == 10)
  assert(round(returns[3]['start_balance'], 5) == 11)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[3]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_TwoRetrunOneOfWhichIsZeroCalculatedWhenBalancesWithCollateralCorrectionGenerated48HoursWith24IntervalPeriodAndOneBalanceIsMissing(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=47)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.5,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=23)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 3)
  assert(returns[1]['return'] == 0.0)
  assert(round(returns[1]['end_balance'], 5) == 11)
  assert(round(returns[1]['start_balance'], 5) == 0)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)
  assert(round(returns[2]['return'], 5) == -0.09531)
  assert(round(returns[2]['end_balance'], 5) == 10)
  assert(round(returns[2]['start_balance'], 5) == 11)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[2]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_ReturnsAreBackfilledWithDepositWithdrawlAndWithCollateralCorrectionAnd24HourIntervalPeriodWhenNoExistingReturns(db_session):
  db_session = db_session
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.5,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_DEPOSIT",
      'income': 1,
      'income_base': 1,
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_WITHDRAW",
      'income': -2.1,
      'income_base': -2.1,
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  print(returns)
  assert(len(returns) == 2)
  assert(returns[0]['return'] == 0)
  assert(round(returns[1]['return'], 5) == 0.00905)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 11)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_SingleRetrunCalculatedWhenDelayedBalancesGenerated48HoursApartWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=49)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=72)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(len(returns) == 2)
  assert(round(returns[1]['return'], 5) == 0.10536)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_SingleRetrunCalculatedAndTimeStampedWithLastBalanceDataWhenBalancesGenerated24HourstWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 11,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )
  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(
    len(returns) == 2
  )
  assert('end_balance' in returns[1])
  assert('timestamp' in returns[1])

  assert(round(returns[1]['return'], 5) == -0.09531)
  assert(round(returns[1]['end_balance'], 5) == 10)
  assert(round(returns[1]['start_balance'], 5) == 11)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_OutlierAndEWMA(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 12},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=0)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 13},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=10)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=20)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 22},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=30)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )
  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(
    len(returns) == 2
  )

  assert(round(returns[1]['return'], 5) == 0.28638)
  assert(round(returns[1]['end_balance'], 5) == 11.98438)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_TransfersBeforeUSDTConvertedPayment(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 9,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 10},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 12},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=0)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=10)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 24},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=20)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 22},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=30)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_DEPOSIT",
      'income_base': 0,
      'income': 12,
      'timestamp': int(((datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=15)).timestamp() * 1000)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )
  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(
    len(returns) == 2
  )

  assert(round(returns[1]['return'], 5) == 0)
  assert(round(returns[1]['end_balance'], 5) == 22)
  assert(round(returns[1]['start_balance'], 5) == 9)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

def test_BaseCcyChange(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'end_balance': 1.01,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=24)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 1},
      'collateral': 1.0,
      'base_ccy': "BTC",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=48)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 65000},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=0)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 65100},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=10)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 65050},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=20)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 65060},
      'collateral': 1.0,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': (datetime.now(timezone.utc) - timedelta(hours=24)) + relativedelta.relativedelta(hour=23, minute=30)
    },
    session=db_session
  )

  DailyReturns(db_session.client, "daily_returns").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )
  returns = list(db_session.client.active_digital.daily_returns.find(
    {'client': "vadym"}, 
    session=db_session)
  )
  assert(
    len(returns) == 2
  )

  tickers = db_session.client.active_digital.tickers.find({'venue': "okx"})
  for item in tickers:
    ticker_value = item['ticker_value']

  ticker = Helper().calc_cross_ccy_ratio("USDT", "BTC", ticker_value)

  assert(round(returns[1]['return'], 5) == round(log(65012.96875 * ticker) - log(1.01), 5))
  assert(round(returns[1]['end_balance'], 5) == 65012.96875)
  assert(round(returns[1]['start_balance'], 5) == 1.01)
  timestamp = datetime.now(timezone.utc) + relativedelta.relativedelta(hour=0, minute=0, second=0, microsecond=0)
  assert(returns[1]['timestamp'].replace(tzinfo=timezone.utc) == timestamp)

  db_session.client.active_digital.daily_returns.delete_many({}, session=db_session)
  db_session.client.active_digital.balances.delete_many({}, session=db_session)

