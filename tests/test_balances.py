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

class TestBalances(unittest.TestCase):
    def setUp(self):
        config = read_config_file('tests/config.yaml')
        mongo_client = MongoClient(config['mongo_host'], config['mongo_port'])
        db = mongo_client['active_digital']
        self.test_collection = db[config['mongo_db_collection']+'_balances']
        self.test_collection.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)['balances']
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)['balances']
        with open('tests/tesa.json') as tesa:
           self.tesa_data = json.load(tesa)['balances']
        with open('tests/teta.json') as teta:
           self.teta_data = json.load(teta)['balances']

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_singleExchangeSingleSubAccountBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.sesa_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data
        )['balance_value']

        result = Balances("test_balances").get(exchange='binance', account='subaccount1')[0]['balance_value']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_singleExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.seta_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.seta_data
        )['balance_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount2',
            balanceValue=self.seta_data
        )['balance_value']

        result1 = Balances("test_balances").get(exchange='binance', account='subaccount1')[0]['balance_value']['subaccount1']
        result2 = Balances("test_balances").get(exchange='binance', account='subaccount2')[0]['balance_value']['subaccount2']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount1'])
        self.assertEqual(result2, self.seta_data['subaccount2'])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_twoExchangeSingleSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.tesa_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.tesa_data
        )['balance_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.tesa_data
        )['balance_value']

        result1 = Balances("test_balances").get(exchange='binance', account='subaccount1')[0]['balance_value']['binance']
        result2 = Balances("test_balances").get(exchange='okx', account='subaccount1')[0]['balance_value']['okx']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.tesa_data['binance'])
        self.assertEqual(result2, self.tesa_data['okx'])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_twoExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.teta_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.teta_data
        )['balance_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount2',
            balanceValue=self.teta_data
        )['balance_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.teta_data
        )['balance_value']

        result1 = Balances("test_balances").get(exchange='binance', account='subaccount1')[0]['balance_value']['binance']['subaccount1']
        result2 = Balances("test_balances").get(exchange='binance', account='subaccount2')[0]['balance_value']['binance']['subaccount2']
        result3 = Balances("test_balances").get(exchange='okx', account='subaccount1')[0]['balance_value']['okx']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.teta_data['binance']['subaccount1'])
        self.assertEqual(result2, self.teta_data['binance']['subaccount2'])
        self.assertEqual(result3, self.teta_data['okx'])

if __name__ == '__main__':
    unittest.main()