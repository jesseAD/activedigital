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

from src.handlers.funding_rates import FundingRates
from src.config import read_config_file
from dotenv import load_dotenv

load_dotenv()

class TestFundingRates(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)

        # read hard-coded values
        with open("tests/frdt.json") as frdt:
            self.frdt_data = json.load(frdt)

    @mock.patch("src.handlers.funding_rates.FundingRates", autospec=True)
    def test_FundingRatesStoredWithSameRunID(self, mock_funding_rates):
        self.db['active_digital']['test_funding_rates'].delete_many({})
        mock_create = mock_funding_rates.return_value.create
        mock_create.return_value = {"funding_rates_value": self.frdt_data}

        # Call mock create function
        FundingRates(self.db, "test_funding_rates").create(
            exchange="binance",
            symbol="BTC/USDT",
            fundingRatesValue=self.frdt_data['BTC/USDT']
        )

        funding_rates = self.db['active_digital']['test_funding_rates'].find({})

        runid = funding_rates[0]['runid']
        for item in funding_rates:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.funding_rates.FundingRates", autospec=True)
    def test_FundingRatesStoredWithDifferentSymbol(self, mock_funding_rates):
        self.db['active_digital']['test_funding_rates'].delete_many({})
        mock_create = mock_funding_rates.return_value.create
        mock_create.return_value = {"funding_rates_value": self.frdt_data}

        # Call mock create function
        FundingRates(self.db, "test_funding_rates").create(
            exchange="binance",
            symbol="BTC/USDT",
            fundingRatesValue=self.frdt_data['BTC/USDT']
        )
        FundingRates(self.db, "test_funding_rates").create(
            exchange="binance",
            symbol="ETH/USDT",
            fundingRatesValue=self.frdt_data['ETH/USDT']
        )

        funding_rates_1 = list(self.db['active_digital']['test_funding_rates'].find({'symbol': 'BTC/USDT'}))
        funding_rates_2 = list(self.db['active_digital']['test_funding_rates'].find({'symbol': 'ETH/USDT'}))

        self.assertEqual(len(self.frdt_data['BTC/USDT']), len(funding_rates_1))
        self.assertEqual(len(self.frdt_data['ETH/USDT']), len(funding_rates_2))

if __name__ == "__main__":
    unittest.main()
