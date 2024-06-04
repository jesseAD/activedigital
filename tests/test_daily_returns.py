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

def test_TwoRetrunCalculatedWhenBalancesWithWithdrawlGenerated48HoursWith24IntervalPeriod(db_session):
  db_session = db_session
  db_session.client.active_digital.daily_returns.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'return': 1.0,
      'runid': 0,
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=17)
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
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=17)
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
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=9)
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
      'timestamp': datetime.now(timezone.utc) - timedelta(hours=1)
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