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

from src.handlers.funding_contributions import FundingContributions
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

def test_ZeroCalculatedWhenLessThanIntervalPeriod(db_session):
  db_session = db_session
  now = datetime.now(timezone.utc)
  base_time = datetime(now.year, now.month, now.day, int(now.hour / config['funding_contributions']['period']['okx']) * config['funding_contributions']['period']['okx']).replace(tzinfo=timezone.utc)
  db_session.client.active_digital.funding_contributions.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'funding_contribution': [{'base': "VADYM", 'contribution': 0.01}],
      'runid': 0,
      'timestamp': base_time
    },
    session=db_session
  )

  FundingContributions(db_session.client, "funding_contributions").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )
  assert(
    len(list(db_session.client.active_digital.funding_contributions.find(
      {'client': "vadym"}, 
      session=db_session)
    )) == 1
  )

def test_PositiveCalculatedWithShortPosition(db_session):
  db_session = db_session
  now = datetime.now(timezone.utc)
  base_time = datetime(now.year, now.month, now.day, int(now.hour / config['funding_contributions']['period']['okx']) * config['funding_contributions']['period']['okx']).replace(tzinfo=timezone.utc)
  db_session.client.active_digital.funding_contributions.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'funding_contribution': [{'base': "VADYM", 'contribution': 0.01}],
      'runid': 0,
      'timestamp': base_time - timedelta(hours=8)
    },
    session=db_session
  )

  db_session.client.active_digital.funding_rates.insert_one(
    {
      'venue': "okx",
      'symbol': "VADYM/USDT",
      'funding_rates_value': {
        'fundingRate': 0.00002,
        'scalar': 1000,
        'timestamp': int(base_time.timestamp() * 1000),
        'base': "VADYM",
        'quote': "USDT"
      },
      'runid': 0,
      'timestamp': base_time
    },
    session=db_session
  )
  db_session.client.active_digital.funding_rates.insert_one(
    {
      'venue': "okx",
      'symbol': "ANDY/USDT",
      'funding_rates_value': {
        'fundingRate': 0.00001,
        'scalar': 1000,
        'timestamp': int(base_time.timestamp() * 1000),
        'base': "ANDY",
        'quote': "USDT"
      },
      'runid': 0,
      'timestamp': base_time
    },
    session=db_session
  )

  db_session.client.active_digital.positions.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'position_value': [{
        'base': "VADYM",
        'side': "short",
        'notional': 100
      }, {
        'base': "ANDY",
        'side': "short",
        'notional': 300
      }],
      'runid': 0,
      'timestamp': base_time - timedelta(hours=1)
    },
    session=db_session
  )

  FundingContributions(db_session.client, "funding_contributions").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  res =list(db_session.client.active_digital.funding_contributions.find(
    {'client': "vadym"}, 
    session=db_session)
  )

  assert(
    len(res) == 2
  )
  assert(res[1]['funding_contributions'][0]['base']== "ANDY")
  assert(res[1]['funding_contributions'][0]['contribution'] == 0.0075)
  assert(res[1]['funding_contributions'][0]['avg_funding_rate'] == 0.01)
  assert(res[1]['funding_contributions'][1]['base']== "VADYM")
  assert(res[1]['funding_contributions'][1]['contribution'] == 0.005)
  assert(res[1]['funding_contributions'][1]['avg_funding_rate'] == 0.02)

def testNegativeCalculatedWithLongPosition(db_session):
  db_session = db_session
  now = datetime.now(timezone.utc)
  base_time = datetime(now.year, now.month, now.day, int(now.hour / config['funding_contributions']['period']['okx']) * config['funding_contributions']['period']['okx']).replace(tzinfo=timezone.utc)
  db_session.client.active_digital.funding_contributions.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'funding_contribution': [{'base': "VADYM", 'contribution': 0.01}],
      'runid': 0,
      'timestamp': base_time - timedelta(hours=8)
    },
    session=db_session
  )

  db_session.client.active_digital.funding_rates.insert_one(
    {
      'venue': "okx",
      'symbol': "VADYM/USDT",
      'funding_rates_value': {
        'fundingRate': 0.00002,
        'scalar': 1000,
        'timestamp': int(base_time.timestamp() * 1000),
        'base': "VADYM",
        'quote': "USDT"
      },
      'runid': 0,
      'timestamp': base_time
    },
    session=db_session
  )
  db_session.client.active_digital.funding_rates.insert_one(
    {
      'venue': "okx",
      'symbol': "ANDY/USDT",
      'funding_rates_value': {
        'fundingRate': 0.00001,
        'scalar': 1000,
        'timestamp': int(base_time.timestamp() * 1000),
        'base': "ANDY",
        'quote': "USDT"
      },
      'runid': 0,
      'timestamp': base_time
    },
    session=db_session
  )

  db_session.client.active_digital.positions.insert_one(
    {
      'client': "vadym",
      'venue': "okx",
      'account': "subaccount",
      'position_value': [{
        'base': "VADYM",
        'side': "long",
        'notional': 100
      }, {
        'base': "ANDY",
        'side': "long",
        'notional': 300
      }],
      'runid': 0,
      'timestamp': base_time - timedelta(hours=1)
    },
    session=db_session
  )

  FundingContributions(db_session.client, "funding_contributions").create(
    client="vadym", exch=None, exchange="okx", account="subaccount", session=db_session
  )

  res =list(db_session.client.active_digital.funding_contributions.find(
    {'client': "vadym"}, 
    session=db_session)
  )

  assert(
    len(res) == 2
  )
  assert(res[1]['funding_contributions'][0]['base']== "ANDY")
  assert(res[1]['funding_contributions'][0]['contribution'] == -0.0075)
  assert(res[1]['funding_contributions'][0]['avg_funding_rate'] == -0.01)
  assert(res[1]['funding_contributions'][1]['base']== "VADYM")
  assert(res[1]['funding_contributions'][1]['contribution'] == -0.005)
  assert(res[1]['funding_contributions'][1]['avg_funding_rate'] == -0.02)

