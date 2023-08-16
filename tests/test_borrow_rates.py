import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.borrow_rates import BorrowRates
from src.config import read_config_file


class TestBorrowRates(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file("tests/config.yaml")
        self.mongo_client = MongoClient(
            self.config["mongo_host"], self.config["mongo_port"]
        )
        self.db = self.mongo_client["active_digital"]
        self.test_collection = self.db[
            self.config["mongo_db_collection"] + "_positions"
        ]
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config["mongo_db_collection"] + "_balances"]
        self.test_collection.delete_many({})
        self.test_collection = self.db[
            self.config["mongo_db_collection"] + "_instruments"
        ]
        self.test_collection.delete_many({})
        self.test_collection = self.db[
            self.config["mongo_db_collection"] + "_leverages"
        ]
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config["mongo_db_collection"] + "_tickers"]
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config["mongo_db_collection"] + "_runs"]
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config["mongo_db_collection"] + "_test"]
        self.test_collection.delete_many({})
        self.test_collection = self.db[
            self.config["mongo_db_collection"] + "_borrow_rates"
        ]
        self.test_collection.delete_many({})
        # read hard-coded values
        with open("tests/brdt.json") as brdt:
            self.brdt_data = json.load(brdt)

    @mock.patch("src.handlers.borrow_rates.BorrowRates", autospec=True)
    def test_BorrowRatesStoredWithSameRunID(self, mock_borrow_rates):
        self.test_collection.delete_many({})
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

        borrow_rates = self.db[self.config["mongo_db_collection"] + "_borrow_rates"].find()

        runid = borrow_rates[0]['runid']
        for item in borrow_rates:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.borrow_rates.BorrowRates", autospec=True)
    def test_BorrowRatesStoredWithDifferentCode(self, mock_borrow_rates):
        self.test_collection.delete_many({})
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

        borrow_rates_usdt = list(self.db[self.config["mongo_db_collection"] + "_borrow_rates"].find({'code': 'USDT'}))
        borrow_rates_btc = list(self.db[self.config["mongo_db_collection"] + "_borrow_rates"].find({'code': 'BTC'}))

        self.assertEqual(len(self.brdt_data['USDT']), len(borrow_rates_usdt))
        self.assertEqual(len(self.brdt_data['BTC']), len(borrow_rates_btc))

if __name__ == "__main__":
    unittest.main()
