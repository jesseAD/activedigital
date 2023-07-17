import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.positions import Positions

class TestPositions(unittest.TestCase):
    def setUp(self):
        mongo_client = MongoClient('localhost', 27017)
        db = mongo_client['positions']
        self.test_collection = db['test']
        self.test_collection.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)        

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_singleExchangeSingleSubAccountPositionsStoredToMongoDb(self, mock_positions):
        mock_create = mock_positions.return_value.create
        mock_create.return_value = {'accountValue': self.sesa_data}

        # Call mock create function using existing json data
        Positions("test").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            accountValue=self.sesa_data
        )['accountValue']

        result = Positions("test").get(exchange='binance', position_type='long', account='subaccount1')[0]['accountValue']
        # Iterate over the result and compare the values
        for key, value in self.sesa_data.items():
            self.assertEqual(result[key], value)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_singleExchangeTwoSubAccountsPositionsStoredToMongoDb(self, mock_positions):
        mock_create = mock_positions.return_value.create
        mock_create.return_value = {'accountValue': self.seta_data}

        # Call mock create function using existing json data
        Positions("test").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            accountValue=self.seta_data
        )['accountValue']

        Positions("test").create(
            exchange='binance',
            positionType='long',
            sub_account='subaccount2',
            accountValue=self.seta_data
        )['accountValue']

        result1 = Positions("test").get(exchange='binance', position_type='long', account='subaccount1')[0]['accountValue']['subaccount1']
        result2 = Positions("test").get(exchange='binance', position_type='long', account='subaccount2')[0]['accountValue']['subaccount2']

        # Iterate over the result and compare the values
        for key, value in self.seta_data["subaccount1"].items():
            self.assertEqual(result1[key], value)
        for key, value in self.seta_data["subaccount2"].items():
            self.assertEqual(result2[key], value)

    # def test_twoExchangeSingleSubAccountsPositionsStoredToMongoDb(self):
    #     with mock.patch('src.lib.config.get_data_collectors'):            
    #         position1 = Positions.create_test(
    #             exchange='binance',
    #             positionType='long',
    #             sub_account='subaccount1'
    #         )
    #         position2 = Positions.create_test(
    #             exchange='okx',
    #             positionType='long',
    #             sub_account='subaccount1'
    #         )
            
    #         stored_result1 = Positions.get_test(
    #             exchange='binance',
    #             position_type='long',
    #             account='subaccount1'
    #         )
    #         stored_result2 = Positions.get_test(
    #             exchange='okx',
    #             position_type='long',
    #             account='subaccount1'
    #         )

    #         assert position1 == stored_result1 and position2 == stored_result2

    # def test_twoExchangeaTwoSubAccountsPositionsStoredToMongoDb(self):
    #     position1 = Positions.create_test(
    #         exchange='binance',
    #         positionType='long',
    #         sub_account='subaccount1'
    #     )
    #     position2 = Positions.create_test(
    #         exchange='binance',
    #         positionType='long',
    #         sub_account='subaccount2'
    #     )
    #     position3 = Positions.create_test(
    #         exchange='okx',
    #         positionType='long',
    #         sub_account='subaccount1'
    #     )
        
    #     stored_result1 = Positions.get_test(
    #         exchange='binance',
    #         position_type='long',
    #         account='subaccount1'
    #     )
    #     stored_result2 = Positions.get_test(
    #         exchange='binance',
    #         position_type='long',
    #         account='subaccount2'
    #     )
    #     stored_result3 = Positions.get_test(
    #         exchange='okx',
    #         position_type='long',
    #         account='subaccount1'
    #     )

    #     assert position1 == stored_result1 and position2 == stored_result2 and position3 == stored_result3

if __name__ == '__main__':
    unittest.main()