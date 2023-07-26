import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.tickers import Tickers
from src.config import read_config_file

class TestTickers(unittest.TestCase):
    def setUp(self):
        config = read_config_file('tests/config.yaml')
        mongo_client = MongoClient(config['mongo_host'], config['mongo_port'])
        db = mongo_client['active_digital']
        self.test_collection = db[config['mongo_db_collection']+'_tickers']
        self.test_collection.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)['tickers']
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)['tickers']
        with open('tests/tesa.json') as tesa:
           self.tesa_data = json.load(tesa)['tickers']
        with open('tests/teta.json') as teta:
           self.teta_data = json.load(teta)['tickers']

    @mock.patch('src.handlers.tickers.Tickers', autospec=True)
    def test_singleExchangeSingleSubAccountTickersStoredToMongoDb(self, mock_tickers):
        self.test_collection.delete_many({})
        mock_create = mock_tickers.return_value.create
        mock_create.return_value = {'tickerValue': self.sesa_data}

        # Call mock create function using existing json data
        Tickers("test_tickers").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            tickerValue=self.sesa_data
        )['tickerValue']

        result = Tickers("test_tickers").get(exchange='binance', position_type='long', account='subaccount1')[0]['tickerValue']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.tickers.Tickers', autospec=True)
    def test_singleExchangeTwoSubAccountsTickersStoredToMongoDb(self, mock_tickers):
        self.test_collection.delete_many({})
        mock_create = mock_tickers.return_value.create
        mock_create.return_value = {'tickerValue': self.seta_data}

        # Call mock create function using existing json data
        Tickers("test_tickers").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            tickerValue=self.seta_data
        )['tickerValue']

        Tickers("test_tickers").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount2',
            tickerValue=self.seta_data
        )['tickerValue']

        result1 = Tickers("test_tickers").get(exchange='binance', position_type='long', account='subaccount1')[0]['tickerValue']['subaccount1']
        result2 = Tickers("test_tickers").get(exchange='binance', position_type='long', account='subaccount2')[0]['tickerValue']['subaccount2']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount1'])
        self.assertEqual(result2, self.seta_data['subaccount2'])

    @mock.patch('src.handlers.tickers.Tickers', autospec=True)
    def test_twoExchangeSingleSubAccountsTickersStoredToMongoDb(self, mock_tickers):
        self.test_collection.delete_many({})
        mock_create = mock_tickers.return_value.create
        mock_create.return_value = {'tickerValue': self.tesa_data}

        # Call mock create function using existing json data
        Tickers("test_tickers").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            tickerValue=self.tesa_data
        )['tickerValue']

        Tickers("test_tickers").create(
            exchange='okx',
            positionType='long',
            sub_account='subaccount1',
            tickerValue=self.tesa_data
        )['tickerValue']

        result1 = Tickers("test_tickers").get(exchange='binance', position_type='long', account='subaccount1')[0]['tickerValue']['binance']
        result2 = Tickers("test_tickers").get(exchange='okx', position_type='long', account='subaccount1')[0]['tickerValue']['okx']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.tesa_data['binance'])
        self.assertEqual(result2, self.tesa_data['okx'])

    @mock.patch('src.handlers.tickers.Tickers', autospec=True)
    def test_twoExchangeTwoSubAccountsTickersStoredToMongoDb(self, mock_tickers):
        self.test_collection.delete_many({})
        mock_create = mock_tickers.return_value.create
        mock_create.return_value = {'tickerValue': self.teta_data}

        # Call mock create function using existing json data
        Tickers("test_tickers").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            tickerValue=self.teta_data
        )['tickerValue']

        Tickers("test_tickers").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount2',
            tickerValue=self.teta_data
        )['tickerValue']

        Tickers("test_tickers").create(
            exchange='okx',
            positionType='long',
            sub_account='subaccount1',
            tickerValue=self.teta_data
        )['tickerValue']

        result1 = Tickers("test_tickers").get(exchange='binance', position_type='long', account='subaccount1')[0]['tickerValue']['binance']['subaccount1']
        result2 = Tickers("test_tickers").get(exchange='binance', position_type='long', account='subaccount2')[0]['tickerValue']['binance']['subaccount2']
        result3 = Tickers("test_tickers").get(exchange='okx', position_type='long', account='subaccount1')[0]['tickerValue']['okx']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.teta_data['binance']['subaccount1'])
        self.assertEqual(result2, self.teta_data['binance']['subaccount2'])
        self.assertEqual(result3, self.teta_data['okx'])

if __name__ == '__main__':
    unittest.main()