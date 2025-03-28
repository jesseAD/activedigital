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

from src.handlers.fills import Fills
from src.config import read_config_file
from dotenv import load_dotenv

load_dotenv()

class TestFills(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)

        # read hard-coded values
        with open("tests/fsdt.json") as fsdt:
            self.fsdt_data = json.load(fsdt)

    @mock.patch("src.handlers.fills.Fills", autospec=True)
    def test_FillsStoredWithSameRunID(self, mock_fills):
        self.db['active_digital']['test_fills'].delete_many({})
        mock_create = mock_fills.return_value.create
        mock_create.return_value = {"fills_value": self.fsdt_data}

        # Call mock create function
        Fills(self.db, "test_fills").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            symbols=["BTCUSDT"],
            fillsValue=self.fsdt_data
        )

        fills = self.db['active_digital']['test_fills'].find({})

        runid = fills[0]['runid']
        for item in fills:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.fills.Fills", autospec=True)
    def test_FillsStoredWithDifferentSymbol(self, mock_fills):
        self.db['active_digital']['test_fills'].delete_many({})
        mock_create = mock_fills.return_value.create
        mock_create.return_value = {"fills_value": self.fsdt_data}

        # Call mock create function
        Fills(self.db, "test_fills").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            symbols=["BTCUSDT", "USDTETH", ],
            fillsValue=self.fsdt_data
        )

        fills_1 = list(self.db['active_digital']['test_fills'].find({'symbol': 'BTCUSDT'}))
        fills_2 = list(self.db['active_digital']['test_fills'].find({'symbol': 'USDTETH'}))

        self.assertEqual(len(self.fsdt_data['USDTETH']), len(fills_2))
        self.assertEqual(len(self.fsdt_data['BTCUSDT']), len(fills_1))

if __name__ == "__main__":
    unittest.main()
