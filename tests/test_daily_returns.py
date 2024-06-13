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
  assert(round(returns[1]['return'], 5) == 0.73788)

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
  assert(round(returns[1]['return'], 5) == 0.68072)
  assert(round(returns[2]['return'], 5) == 0.82591)

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
  assert(round(returns[1]['return'], 5) == 0.68072)
  assert(round(returns[2]['return'], 5) == 0.77603)
  assert(round(returns[3]['return'], 5) == 0.62283)

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
  assert(round(returns[1]['return'], 5) == 0.68072)
  assert(round(returns[2]['return'], 5) == 0.77603)

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
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=97)
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
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=74)
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
      'collateral': 1.5,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
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
  assert(round(returns[1]['return'], 5) == 0.68072)
  assert(round(returns[2]['return'], 5) == 0.87804)
  assert(round(returns[3]['return'], 5) == 0.62283)

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
  assert(round(returns[2]['return'], 5) == 0.68072)

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
      'collateral': 1.5,
      'base_ccy': "USDT",
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000)
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
  assert(returns[0]['return'] == 0)
  assert(round(returns[1]['return'], 5) == 0.06004)

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
  assert(round(returns[1]['return'], 5) == 0.68072)

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

  assert(round(returns[1]['return'], 5) == 0.48005)

def test_Outlier(db_session):
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

  assert(round(returns[1]['return'], 5) == 0.20169)