import json
import os
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.helpers import Helper
from src.config import read_config_file
from src.handlers.database_connector import database_connector
from src.lib.db import MongoDB
from dotenv import load_dotenv

load_dotenv()


class TestCrossCcyRatio(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file("tests/config.yaml")

        if os.getenv("mode") == "testing":
            self.tickers_db = MongoDB(self.config["mongo_db"], "tickers")
        else:
            self.tickers_db = database_connector("tickers")

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
