import json
import os
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.fills import Fills
from src.config import read_config_file
from src.handlers.database_connector import database_connector
from src.lib.db import MongoDB
from dotenv import load_dotenv

load_dotenv()

class TestFills(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file("tests/config.yaml")

        if os.getenv("mode") == "testing":
            self.fills_db = MongoDB(self.config['mongo_db'], 'test_fills')
        else:
            self.fills_db = database_connector('test_fills')

        self.fills_db.delete_many({})

        self.mongo_client = MongoClient(
            self.config["mongo_host"], self.config["mongo_port"]
        )
        self.db = self.mongo_client["active_digital"]
        self.test_collection = self.db[
            self.config["mongo_db_collection"] + "_positions"
        ]
        self.test_collection.delete_many({})

        # read hard-coded values
        with open("tests/fsdt.json") as fsdt:
            self.fsdt_data = json.load(fsdt)

    @mock.patch("src.handlers.fills.Fills", autospec=True)
    def test_FillsStoredWithSameRunID(self, mock_fills):
        self.test_collection.delete_many({})
        mock_create = mock_fills.return_value.create
        mock_create.return_value = {"fills_value": self.fsdt_data}

        # Call mock create function
        Fills("test_fills").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            symbols=["BTCUSDT"],
            fillsValue=self.fsdt_data
        )

        fills = self.fills_db.find({})

        runid = fills[0]['runid']
        for item in fills:
            self.assertEqual(runid, item['runid'])

    @mock.patch("src.handlers.fills.Fills", autospec=True)
    def test_FillsStoredWithDifferentSymbol(self, mock_fills):
        self.test_collection.delete_many({})
        mock_create = mock_fills.return_value.create
        mock_create.return_value = {"fills_value": self.fsdt_data}

        # Call mock create function
        Fills("test_fills").create(
            client="deepspace",
            exchange="binance",
            sub_account="subaccount1",
            symbols=["BTCUSDT", "USDTETH", ],
            fillsValue=self.fsdt_data
        )

        fills_1 = list(self.fills_db.find({'symbol': 'BTCUSDT'}))
        fills_2 = list(self.fills_db.find({'symbol': 'USDTETH'}))

        self.assertEqual(len(self.fsdt_data['USDTETH']), len(fills_2))
        self.assertEqual(len(self.fsdt_data['BTCUSDT']), len(fills_1))

if __name__ == "__main__":
    unittest.main()
