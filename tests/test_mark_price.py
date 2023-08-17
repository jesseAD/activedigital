import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.funding_rates import FundingRates
from src.handlers.mark_price import MarkPrices
from src.config import read_config_file


class TestMarkPrices(unittest.TestCase):
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
            self.config["mongo_db_collection"] + "_mark_prices"
        ]
        self.test_collection.delete_many({})
        # read hard-coded values
        with open("tests/mpdt.json") as mpdt:
            self.mpdt_data = json.load(mpdt)

    @mock.patch("src.handlers.mark_price.MarkPrices", autospec=True)
    def test_SameMarkPriceNotStored(self, mock_mark_prices):
        self.test_collection.delete_many({})
        mock_create = mock_mark_prices.return_value.create
        mock_create.return_value = {"mark_prices_value": self.mpdt_data}

        # Call mock create function
        MarkPrices("test_mark_prices").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            markPriceValue=self.mpdt_data
        )

        prev_runid = self.db[self.config["mongo_db_collection"] + "_mark_prices"].find()[0]['runid']

        MarkPrices("test_mark_prices").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            markPriceValue=self.mpdt_data
        )

        mark_prices = self.db[self.config["mongo_db_collection"] + "_mark_prices"].find().sort('runid', -1).limit(1)
        for item in mark_prices:
            latest_runid = item['runid']

        self.assertEqual(prev_runid, latest_runid)

    @mock.patch("src.handlers.mark_price.MarkPrices", autospec=True)
    def test_MarkPriceStoredInPositionDB(self, mock_mark_prices):
        self.test_collection.delete_many({})
        mock_create = mock_mark_prices.return_value.create
        mock_create.return_value = {"mark_prices_value": self.mpdt_data}

        # Call mock create function
        MarkPrices("test_mark_prices").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            markPriceValue=self.mpdt_data
        )

        query = {
            'client': "deepspace",
            'venue': "binance",
            'account': "subaccount1",
        }

        positionsDB = self.db['positions'].find(query).sort('runid', -1).limit(1)
        for item in positionsDB:
            mark_price = item['position_value'][0]['markPrice']

        self.assertIsNotNone(mark_price)

if __name__ == "__main__":
    unittest.main()
