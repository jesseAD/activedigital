import os
import sys

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.balances import Balances
from src.config import read_config_file
from src.handlers.database_connector import database_connector
from src.lib.db import MongoDB
from dotenv import load_dotenv

load_dotenv()
class TestBalances(unittest.TestCase):
    def setUp(self):
        config = read_config_file('tests/config.yaml')

        if os.getenv("mode") == "testing":
            self.balances_db = MongoDB(config['mongo_db'], 'test_balances')
        else:
            self.balances_db = database_connector('test_balances')

        self.balances_db.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)['balances'][0]
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)['balances']
        with open('tests/tesa.json') as tesa:
           self.tesa_data = json.load(tesa)['balances']
        with open('tests/teta.json') as teta:
           self.teta_data = json.load(teta)['balances']

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_SingleExchangeSingleSubAccountBalancesStoredToMongoDb(self, mock_balances):
        self.balances_db.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.sesa_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data
        )['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='binance', account='subaccount1')
        for item in results:
            result = item['balance_value']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_SingleExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.balances_db.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.seta_data}

        # Call mock create function using existing json data
        for _key, _val in self.seta_data.items():
            Balances("test_balances").create(
                client='deepspace',
                exchange='binance',
                sub_account=_key,
                balanceValue=_val[0]
            )['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='binance', account='subaccount1')
        for item in results:
            result1 = item['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='binance', account='subaccount2')
        for item in results:
            result2 = item['balance_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount1'][0])
        self.assertEqual(result2, self.seta_data['subaccount2'][0])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_TwoExchangeSingleSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.balances_db.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.tesa_data}

        # Call mock create function using existing json data
        for _key, _val in self.tesa_data.items():
            Balances("test_balances").create(
                client='deepspace',
                exchange=_key,
                sub_account='subaccount1',
                balanceValue=_val[0]
            )['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='binance', account='subaccount1')
        for item in results:
            result1 = item['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='okx', account='subaccount1')
        for item in results:
            result2 = item['balance_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.tesa_data['binance'][0])
        self.assertEqual(result2, self.tesa_data['okx'][0])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_TwoExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.balances_db.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.teta_data}

        # Call mock create function using existing json data
        for _key, _val in self.teta_data['binance'].items():
            Balances("test_balances").create(
                client='deepspace',
                exchange='binance',
                sub_account=_key,
                balanceValue=_val[0]
            )['balance_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.teta_data['okx'][0]
        )['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='binance', account='subaccount1')
        for item in results:
            result1 = item['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='binance', account='subaccount2')
        for item in results:
            result2 = item['balance_value']

        results = Balances("test_balances").get(client='deepspace', exchange='okx', account='subaccount1')
        for item in results:
            result3 = item['balance_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.teta_data['binance']['subaccount1'][0])
        self.assertEqual(result2, self.teta_data['binance']['subaccount2'][0])
        self.assertEqual(result3, self.teta_data['okx'][0])

if __name__ == '__main__':
    unittest.main()