import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.instruments import Instruments
from src.config import read_config_file

class TestInstruments(unittest.TestCase):
    def setUp(self):
        config = read_config_file('tests/config.yaml')
        mongo_client = MongoClient(config['mongo_host'], config['mongo_port'])
        db = mongo_client['active_digital']
        self.test_collection = db[config['mongo_db_collection']+'_instruments']
        self.test_collection.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)['instruments']
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)['instruments']
        # with open('tests/tesa.json') as tesa:
        #    self.tesa_data = json.load(tesa)['instruments']
        # with open('tests/teta.json') as teta:
        #    self.teta_data = json.load(teta)['instruments']

    @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    def test_singleExchangeSingleSubAccountInstrumentsStoredToMongoDb(self, mock_instruments):
        self.test_collection.delete_many({})
        mock_create = mock_instruments.return_value.create
        mock_create.return_value = {'instrument_value': self.sesa_data}

        # Call mock create function using existing json data
        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            instrumentValue=self.sesa_data
        )['instrument_value']

        result = Instruments("test_instruments").get(exchange='binance', account='subaccount1')[0]['instrument_value']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    def test_singleExchangeTwoSubAccountsInstrumentsStoredToMongoDb(self, mock_instruments):
        self.test_collection.delete_many({})
        mock_create = mock_instruments.return_value.create
        mock_create.return_value = {'instrument_value': self.seta_data}

        # Call mock create function using existing json data
        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            instrumentValue=self.seta_data
        )['instrument_value']

        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount2',
            instrumentValue=self.seta_data
        )['instrument_value']

        result1 = Instruments("test_instruments").get(exchange='binance', account='subaccount1')[0]['instrument_value']['subaccount1']
        result2 = Instruments("test_instruments").get(exchange='binance', account='subaccount2')[0]['instrument_value']['subaccount2']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount1'])
        self.assertEqual(result2, self.seta_data['subaccount2'])

    # @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    # def test_twoExchangeSingleSubAccountsInstrumentsStoredToMongoDb(self, mock_instruments):
    #     self.test_collection.delete_many({})
    #     mock_create = mock_instruments.return_value.create
    #     mock_create.return_value = {'instrumentValue': self.tesa_data}

    #     # Call mock create function using existing json data
    #     Instruments("test_instruments").create(
    #         exchange='binance',
    #         positionType='long',
    #         sub_account='subaccount1',
    #         instrumentValue=self.tesa_data
    #     )['instrumentValue']

    #     Instruments("test_instruments").create(
    #         exchange='okx',
    #         positionType='long',
    #         sub_account='subaccount1',
    #         instrumentValue=self.tesa_data
    #     )['instrumentValue']

    #     result1 = Instruments("test_instruments").get(exchange='binance', position_type='long', account='subaccount1')[0]['instrumentValue']['binance']
    #     result2 = Instruments("test_instruments").get(exchange='okx', position_type='long', account='subaccount1')[0]['instrumentValue']['okx']

    #     # Iterate over the result and compare the values
    #     self.assertEqual(result1, self.tesa_data['binance'])
    #     self.assertEqual(result2, self.tesa_data['okx'])

    # @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    # def test_twoExchangeTwoSubAccountsInstrumentsStoredToMongoDb(self, mock_instruments):
    #     self.test_collection.delete_many({})
    #     mock_create = mock_instruments.return_value.create
    #     mock_create.return_value = {'instrumentValue': self.teta_data}

    #     # Call mock create function using existing json data
    #     Instruments("test_instruments").create(
    #         exchange='binance',
    #         positionType='long',
    #         sub_account='subaccount1',
    #         instrumentValue=self.teta_data
    #     )['instrumentValue']

    #     Instruments("test_instruments").create(
    #         exchange='binance',
    #         positionType='long',
    #         sub_account='subaccount2',
    #         instrumentValue=self.teta_data
    #     )['instrumentValue']

    #     Instruments("test_instruments").create(
    #         exchange='okx',
    #         positionType='long',
    #         sub_account='subaccount1',
    #         instrumentValue=self.teta_data
    #     )['instrumentValue']

    #     result1 = Instruments("test_instruments").get(exchange='binance', position_type='long', account='subaccount1')[0]['instrumentValue']['binance']['subaccount1']
    #     result2 = Instruments("test_instruments").get(exchange='binance', position_type='long', account='subaccount2')[0]['instrumentValue']['binance']['subaccount2']
    #     result3 = Instruments("test_instruments").get(exchange='okx', position_type='long', account='subaccount1')[0]['instrumentValue']['okx']

    #     # Iterate over the result and compare the values
    #     self.assertEqual(result1, self.teta_data['binance']['subaccount1'])
    #     self.assertEqual(result2, self.teta_data['binance']['subaccount2'])
    #     self.assertEqual(result3, self.teta_data['okx'])

if __name__ == '__main__':
    unittest.main()