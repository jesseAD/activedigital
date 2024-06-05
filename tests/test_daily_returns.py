import json
import os, sys
import unittest
from unittest import mock
import pytest
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta

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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
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
    )) == 2
  )

def test_TwoRetrunCalculatedWhenBalancesWithWithdrawlGenerated48HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
      'balance_value': {'base': 10},
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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
    },
    session=db_session
  )
  db_session.client.active_digital.transaction_union.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'incomeType': "COIN_SWAP_WITHDRAW",
      'income': -1,
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=10)).timestamp() * 1000)
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
    )) == 3
  )

def test_ThreeRetrunCalculatedWhenBalancesWithDepositlGenerated72HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=10)).timestamp() * 1000)
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
    )) == 4
  )

def test_TwoRetrunCalculatedWhenBalancesWithCollateralCorrectionGenerated48HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
      'balance_value': {'base': 10},
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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'collateral': 1.1,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
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
    )) == 3
  )

def test_ThreeRetrunCalculatedWhenBalancesWithDepositWithdrawlAndWithCollateralCorrectionlGenerated72HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'collateral': 1.2,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=10)).timestamp() * 1000)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000)
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
    )) == 4
  )

def test_TwoRetrunOneOfWhichIsZeroCalculatedWhenBalancesWithCollateralCorrectionGenerated48HoursWith24IntervalPeriodAndOneBalanceIsMissing(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=23)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'collateral': 1.2,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
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
    )) == 3
  )
  assert(
    list(db_session.client.active_digital.daily_returns.find(
      {'client': "vadym"}, 
      session=db_session))[1]['return'] == 0.0
  )

def test_ReturnsAreBackfilledWithDepositWithdrawlAndWithCollateralCorrectionAnd24HourIntervalPeriodWhenNoExistingReturns(db_session):
  db_session = db_session
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 11},
      'collateral': 1.0,
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
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
    },
    session=db_session
  )
  db_session.client.active_digital.balances.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'balance_value': {'base': 9},
      'collateral': 1.2,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=10)).timestamp() * 1000)
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
      'timestamp': int((datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000)
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
    )) == 2
  )

def test_SingleRetrunCalculatedWhenDelayedBalancesGenerated48HoursApartWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
      'collateral': 1.5,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=25)
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
    )) == 2
  )

def test_SingleRetrunCalculatedAndTimeStampedWithLastBalanceDataWhenBalancesGenerated24HourstWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
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
  assert('prev_balance' in returns[1])
  assert('timestamp' in returns[1])

