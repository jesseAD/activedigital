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

from src.handlers.helpers import Helper
from src.config import read_config_file
from dotenv import load_dotenv

load_dotenv()


class TestCrossCcyRatio(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.tickers_db = MongoClient(mongo_uri)['active_digital']['tickers']

    @mock.patch("src.handlers.helpers.Helper", autospec=True)
    def test_SameBaseAndQuote(self, mock_helper):
        # Call mock create function
        tickers = self.tickers_db.find({"venue": "binance"}).sort("_id", -1).limit(1)
        latest_ticker = None
        for item in tickers:
            latest_ticker = item["ticker_value"]

        self.assertEqual(Helper().calc_cross_ccy_ratio("BTC", "BTC", latest_ticker), 1)
        self.assertEqual(Helper().calc_cross_ccy_ratio("USD", "USD", latest_ticker), 1)
        self.assertEqual(
            Helper().calc_cross_ccy_ratio("USDT", "USDT", latest_ticker), 1
        )

    @mock.patch("src.handlers.helpers.Helper", autospec=True)
    def test_DifferntBaseAndQuoteUsingTickerValues(self, mock_helper):
        # Call mock create function
        tickers = self.tickers_db.find({"venue": "binance"}).sort("_id", -1).limit(1)
        latest_ticker = None
        for item in tickers:
            latest_ticker = item["ticker_value"]

        self.assertEqual(
            Helper().calc_cross_ccy_ratio("BTC", "USDT", latest_ticker),
            latest_ticker["BTC/USDT"]["last"],
        )
        self.assertEqual(
            Helper().calc_cross_ccy_ratio("ETH", "USDT", latest_ticker),
            latest_ticker["ETH/USDT"]["last"],
        )

    @mock.patch("src.handlers.helpers.Helper", autospec=True)
    def test_DifferntBaseAndQuoteUsingCrossRate(self, mock_helper):
        # Call mock create function
        tickers = self.tickers_db.find({"venue": "binance"}).sort("_id", -1).limit(1)
        latest_ticker = None
        for item in tickers:
            latest_ticker = item["ticker_value"]

        self.assertEqual(
            Helper().calc_cross_ccy_ratio("BTC", "ETH", latest_ticker),
            latest_ticker["BTC/USDT"]["last"] / latest_ticker["ETH/USDT"]["last"],
        )
        self.assertEqual(
            Helper().calc_cross_ccy_ratio("ETH", "BTC", latest_ticker),
            latest_ticker["ETH/USDT"]["last"] / latest_ticker["BTC/USDT"]["last"],
        )


if __name__ == "__main__":
    unittest.main()
