import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.balances import Balances
from src.lib.config import read_config_file

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
        mock_create.return_value = {'balanceValue': self.sesa_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            balanceValue=self.sesa_data
        )['balanceValue']

        result = Balances("test_balances").get(exchange='binance', position_type='long', account='subaccount1')[0]['balanceValue']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_singleExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balanceValue': self.seta_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            balanceValue=self.seta_data
        )['balanceValue']

        Balances("test_balances").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount2',
            balanceValue=self.seta_data
        )['balanceValue']

        result1 = Balances("test_balances").get(exchange='binance', position_type='long', account='subaccount1')[0]['balanceValue']['subaccount1']
        result2 = Balances("test_balances").get(exchange='binance', position_type='long', account='subaccount2')[0]['balanceValue']['subaccount2']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount1'])
        self.assertEqual(result2, self.seta_data['subaccount2'])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_twoExchangeSingleSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balanceValue': self.tesa_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            balanceValue=self.tesa_data
        )['balanceValue']

        Balances("test_balances").create(
            exchange='okx',
            positionType='long',
            sub_account='subaccount1',
            balanceValue=self.tesa_data
        )['balanceValue']

        result1 = Balances("test_balances").get(exchange='binance', position_type='long', account='subaccount1')[0]['balanceValue']['binance']
        result2 = Balances("test_balances").get(exchange='okx', position_type='long', account='subaccount1')[0]['balanceValue']['okx']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.tesa_data['binance'])
        self.assertEqual(result2, self.tesa_data['okx'])

    @mock.patch('src.handlers.balances.Balances', autospec=True)
    def test_twoExchangeTwoSubAccountsBalancesStoredToMongoDb(self, mock_balances):
        self.test_collection.delete_many({})
        mock_create = mock_balances.return_value.create
        mock_create.return_value = {'balanceValue': self.teta_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            balanceValue=self.teta_data
        )['balanceValue']

        Balances("test_balances").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount2',
            balanceValue=self.teta_data
        )['balanceValue']

        Balances("test_balances").create(
            exchange='okx',
            positionType='long',
            sub_account='subaccount1',
            balanceValue=self.teta_data
        )['balanceValue']

        result1 = Balances("test_balances").get(exchange='binance', position_type='long', account='subaccount1')[0]['balanceValue']['binance']['subaccount1']
        result2 = Balances("test_balances").get(exchange='binance', position_type='long', account='subaccount2')[0]['balanceValue']['binance']['subaccount2']
        result3 = Balances("test_balances").get(exchange='okx', position_type='long', account='subaccount1')[0]['balanceValue']['okx']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.teta_data['binance']['subaccount1'])
        self.assertEqual(result2, self.teta_data['binance']['subaccount2'])
        self.assertEqual(result3, self.teta_data['okx'])

if __name__ == '__main__':
    unittest.main()