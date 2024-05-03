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
from dotenv import load_dotenv

load_dotenv()
class TestBalances(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)
        # 

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
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.sesa_data}

        # Call mock create function using existing json data
        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.sesa_data
        )

        results = Balances(self.db, "test_balances").get(client='rundmc', exchange='okx', account='subaccount1')
        for item in results:
            result = item['balance_value']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_SingleExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.seta_data}

        # Call mock create function using existing json data
        for _key, _val in self.seta_data.items():
            Balances(self.db, "test_balances").create(
                client='rundmc',
                exchange='binance',
                sub_account=_key,
                balanceValue=_val[0]
            )

        results = Balances(self.db, "test_balances").get(client='rundmc', exchange='binance', account='subaccount4')
        for item in results:
            result1 = item['balance_value']

        results = Balances(self.db, "test_balances").get(client='rundmc', exchange='binance', account='subaccount6')
        for item in results:
            result2 = item['balance_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount4'][0])
        self.assertEqual(result2, self.seta_data['subaccount6'][0])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_TwoExchangeSingleSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.tesa_data}

        # Call mock create function using existing json data
        for _key, _val in self.tesa_data.items():
            Balances(self.db, "test_balances").create(
                client='wizardly',
                exchange=_key,
                sub_account='submn',
                balanceValue=_val[0]
            )

        results = Balances(self.db, "test_balances").get(client='wizardly', exchange='binance', account='submn')
        for item in results:
            result1 = item['balance_value']

        results = Balances(self.db, "test_balances").get(client='wizardly', exchange='okx', account='submn')
        for item in results:
            result2 = item['balance_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.tesa_data['binance'][0])
        self.assertEqual(result2, self.tesa_data['okx'][0])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_TwoExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balance_value': self.teta_data}

        # Call mock create function using existing json data
        for _key, _val in self.teta_data['binance'].items():
            Balances(self.db, "test_balances").create(
                client='rundmc',
                exchange='binance',
                sub_account=_key,
                balanceValue=_val[0]
            )

        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.teta_data['okx'][0]
        )

        results = Balances(self.db, "test_balances").get(client='rundmc', exchange='binance', account='subaccount4')
        for item in results:
            result1 = item['balance_value']

        results = Balances(self.db, "test_balances").get(client='rundmc', exchange='binance', account='subaccount6')
        for item in results:
            result2 = item['balance_value']

        results = Balances(self.db, "test_balances").get(client='rundmc', exchange='okx', account='subaccount1')
        for item in results:
            result3 = item['balance_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.teta_data['binance']['subaccount4'][0])
        self.assertEqual(result2, self.teta_data['binance']['subaccount6'][0])
        self.assertEqual(result3, self.teta_data['okx'][0])

if __name__ == '__main__':
    unittest.main()