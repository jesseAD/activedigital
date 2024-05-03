import json

import os
import sys

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.borrow_rates import BorrowRates
from src.config import read_config_file
from dotenv import load_dotenv

load_dotenv()

class TestBorrowRates(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)

        # read hard-coded values
        with open("tests/brdt.json") as brdt:
            self.brdt_data = json.load(brdt)

    @mock.patch("src.handlers.borrow_rates.BorrowRates", autospec=True)
    def test_BorrowRatesStoredWithSameRunID(self, mock_borrow_rates):
        self.db['active_digital']['test_borrow_rates'].delete_many({})
        mock_create = mock_borrow_rates.return_value.create
        mock_create.return_value = {"borrow_rates_value": self.brdt_data}

        # Call mock create function
        BorrowRates(self.db, "test_borrow_rates").create(
            exchange="binance",
            code="USDT",
            borrowRatesValue=self.brdt_data['USDT']
        )

        borrow_rates = self.db['active_digital']['test_borrow_rates'].find({})

        runid = borrow_rates[0]['runid']
        for item in borrow_rates:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.borrow_rates.BorrowRates", autospec=True)
    def test_BorrowRatesStoredWithDifferentCode(self, mock_borrow_rates):
        self.db['active_digital']['test_borrow_rates'].delete_many({})
        mock_create = mock_borrow_rates.return_value.create
        mock_create.return_value = {"borrow_rates_value": self.brdt_data}

        # Call mock create function
        BorrowRates(self.db, "test_borrow_rates").create(
            exchange="binance",
            code="USDT",
            borrowRatesValue=self.brdt_data['USDT']
        )
        BorrowRates(self.db, "test_borrow_rates").create(
            exchange="binance",
            code="BTC",
            borrowRatesValue=self.brdt_data['BTC']
        )

        borrow_rates_usdt = list(self.db['active_digital']['test_borrow_rates'].find({'code': 'USDT'}))
        borrow_rates_btc = list(self.db['active_digital']['test_borrow_rates'].find({'code': 'BTC'}))

        self.assertEqual(len(self.brdt_data['USDT']), len(borrow_rates_usdt))
        self.assertEqual(len(self.brdt_data['BTC']), len(borrow_rates_btc))

if __name__ == "__main__":
    unittest.main()
