import json
import os
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.funding_rates import FundingRates
from src.config import read_config_file
from src.handlers.database_connector import database_connector
from src.lib.db import MongoDB
from dotenv import load_dotenv

load_dotenv()

class TestFundingRates(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file("tests/config.yaml")

        if os.getenv("mode") == "testing":
            self.funding_rates_db = MongoDB(self.config['mongo_db'], 'test_funding_rates')
        else:
            self.funding_rates_db = database_connector('test_funding_rates')

        self.funding_rates_db.delete_many({})

        # read hard-coded values
        with open("tests/frdt.json") as frdt:
            self.frdt_data = json.load(frdt)

    @mock.patch("src.handlers.funding_rates.FundingRates", autospec=True)
    def test_FundingRatesStoredWithSameRunID(self, mock_funding_rates):
        self.funding_rates_db.delete_many({})
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

        funding_rates = self.funding_rates_db.find({})

        runid = funding_rates[0]['runid']
        for item in funding_rates:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.funding_rates.FundingRates", autospec=True)
    def test_FundingRatesStoredWithDifferentSymbol(self, mock_funding_rates):
        self.funding_rates_db.delete_many({})
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

        funding_rates_1 = list(self.funding_rates_db.find({'symbol': 'BTCUSDT'}))
        funding_rates_2= list(self.funding_rates_db.find({'symbol': 'BTC/USDT'}))

        self.assertEqual(len(self.frdt_data['BTC/USDT:USDT']), len(funding_rates_2))
        self.assertEqual(len(self.frdt_data['BTCUSDT']), len(funding_rates_1))

if __name__ == "__main__":
    unittest.main()
