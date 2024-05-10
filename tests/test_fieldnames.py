import os
import sys

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.transactions import Transactions
from src.handlers.positions import Positions
from src.handlers.balances import Balances
from src.handlers.leverages import Leverages
from src.config import read_config_file

class TestFieldNames(unittest.TestCase):
  def setUp(self):
    self.config = read_config_file('tests/config.yaml')

    mongo_uri = None

    if os.getenv("mode") == "prod":
      mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
    self.db = MongoClient(mongo_uri)

    with open('tests/fn_transactions.json') as data:
      self.transaction_data = json.load(data)
    with open('tests/fn_positions.json') as data:
      self.position_data = json.load(data)

  @mock.patch('src.handlers.transactions.Transactions', autospec=True)
  def test_TransactionsFiledName(self, mock_transactions):
    self.db['active_digital']['test_transactions'].delete_many({})

    res = Transactions(self.db, "test_transactions").create(
      client="rundmc", exchange="okx", sub_account="subaccount1", transaction_value=self.transaction_data['okx']
    )
    self.assertEqual(res, True)

    res = Transactions(self.db, "test_transactions").create(
      client="rundmc", exchange="binance", sub_account="subaccount4", transaction_value=self.transaction_data['binance']
    )
    self.assertEqual(res, True)

    res = Transactions(self.db, "test_transactions").create(
      client="besttrader", exchange="bybit", sub_account="sub1", transaction_value=self.transaction_data['bybit']
    )
    self.assertEqual(res, True)

    res = Transactions(self.db, "test_transactions").create(
      client="wilbur", exchange="huobi", sub_account="subbasis1", transaction_value=self.transaction_data['huobi']
    )
    self.assertEqual(res, True)

  @mock.patch('src.handlers.positions.Positions', autospec=True)
  def test_PositionsFiledName(self, mock_positions):
    self.db['active_digital']['test_positions'].delete_many({})

    res = Positions(self.db, "test_positions").create(
      client="rundmc", exchange="okx", sub_account="subaccount1", position_value=self.position_data
    )
    self.assertEqual(res, True)