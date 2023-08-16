import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.funding_rates import FundingRates
from src.config import read_config_file


class TestFundingRates(unittest.TestCase):
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
            self.config["mongo_db_collection"] + "_funding_rates"
        ]
        self.test_collection.delete_many({})
        # read hard-coded values
        with open("tests/frdt.json") as frdt:
            self.frdt_data = json.load(frdt)

    @mock.patch("src.handlers.funding_rates.FundingRates", autospec=True)
    def test_FundingRatesStoredWithSameRunID(self, mock_funding_rates):
        self.test_collection.delete_many({})
        mock_create = mock_funding_rates.return_value.create
        mock_create.return_value = {"funding_rates_value": self.frdt_data}

        # Call mock create function
        FundingRates("test_funding_rates").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            symbols=["BTC/USDT:USDT"],
            fundingRatesValue=self.frdt_data
        )

        funding_rates = self.db[self.config["mongo_db_collection"] + "_funding_rates"].find()

        runid = funding_rates[0]['runid']
        for item in funding_rates:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.funding_rates.FundingRates", autospec=True)
    def test_FundingRatesStoredWithDifferentSymbol(self, mock_funding_rates):
        self.test_collection.delete_many({})
        mock_create = mock_funding_rates.return_value.create
        mock_create.return_value = {"funding_rates_value": self.frdt_data}

        # Call mock create function
        FundingRates("test_funding_rates").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            symbols=["BTCUSDT", "BTC/USDT:USDT", ],
            fundingRatesValue=self.frdt_data
        )

        funding_rates_1 = list(self.db[self.config["mongo_db_collection"] + "_funding_rates"].find({'symbol': 'BTCUSDT'}))
        funding_rates_2= list(self.db[self.config["mongo_db_collection"] + "_funding_rates"].find({'symbol': 'BTC/USDT:USDT'}))

        self.assertEqual(len(self.frdt_data['BTC/USDT:USDT']), len(funding_rates_2))
        self.assertEqual(len(self.frdt_data['BTCUSDT']), len(funding_rates_1))

if __name__ == "__main__":
    unittest.main()
