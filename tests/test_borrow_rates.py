import json
import os
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.borrow_rates import BorrowRates
from src.config import read_config_file
from src.handlers.database_connector import database_connector
from src.lib.db import MongoDB
from dotenv import load_dotenv

load_dotenv()

class TestBorrowRates(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file("tests/config.yaml")

        if os.getenv("mode") == "testing":
            self.borrow_rates_db = MongoDB(self.config['mongo_db'], 'test_borrow_rates')
        else:
            self.borrow_rates_db = database_connector('test_borrow_rates')

        self.borrow_rates_db.delete_many({})
        # read hard-coded values
        with open("tests/brdt.json") as brdt:
            self.brdt_data = json.load(brdt)

    @mock.patch("src.handlers.borrow_rates.BorrowRates", autospec=True)
    def test_BorrowRatesStoredWithSameRunID(self, mock_borrow_rates):
        self.borrow_rates_db.delete_many({})
        mock_create = mock_borrow_rates.return_value.create
        mock_create.return_value = {"borrow_rates_value": self.brdt_data}

        # Call mock create function
        BorrowRates("test_borrow_rates").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            codes=["USDT"],
            borrowRatesValue=self.brdt_data
        )

        borrow_rates = self.borrow_rates_db.find({})

        runid = borrow_rates[0]['runid']
        for item in borrow_rates:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.borrow_rates.BorrowRates", autospec=True)
    def test_BorrowRatesStoredWithDifferentCode(self, mock_borrow_rates):
        self.borrow_rates_db.delete_many({})
        mock_create = mock_borrow_rates.return_value.create
        mock_create.return_value = {"borrow_rates_value": self.brdt_data}

        # Call mock create function
        BorrowRates("test_borrow_rates").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            codes=["USDT", "BTC"],
            borrowRatesValue=self.brdt_data
        )

        borrow_rates_usdt = list(self.borrow_rates_db.find({'code': 'USDT'}))
        borrow_rates_btc = list(self.borrow_rates_db.find({'code': 'BTC'}))

        self.assertEqual(len(self.brdt_data['USDT']), len(borrow_rates_usdt))
        self.assertEqual(len(self.brdt_data['BTC']), len(borrow_rates_btc))

if __name__ == "__main__":
    unittest.main()
