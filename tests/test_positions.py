import json
import unittest
from unittest import mock
from pymongo import MongoClient

import os
import sys

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

from src.handlers.positions import Positions
from src.config import read_config_file

class TestPositions(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)['positions']
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)['positions']

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_singleExchangeSingleSubAccountPositionsStoredToMongoDb(self, mock_positions):
        self.db['active_digital']['test_positions'].delete_many({})
        mock_create = mock_positions.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data}

        # Call mock create function using existing json data
        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='binance',
            sub_account='subaccount4',
            position_value=self.sesa_data
        )

        result = list(
            self.db['active_digital']['test_positions'].find({'client': "rundmc", 'venue': "binance", 'account': "subaccount4"})
        )[0]['position_value']
        self.assertEqual(result, self.sesa_data)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_singleExchangeTwoSubAccountsPositionsStoredToMongoDb(self, mock_positions):
        self.db['active_digital']['test_positions'].delete_many({})
        mock_create = mock_positions.return_value.create
        mock_create.return_value = {'position_value': self.seta_data}

        # Call mock create function using existing json data
        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='binance',
            sub_account='subaccount4',
            position_value=self.seta_data['subaccount4']
        )

        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='binance',
            sub_account='subaccount6',
            position_value=self.seta_data['subaccount6']
        )
        
        result1 = list(
            self.db['active_digital']['test_positions'].find({'client': "rundmc", 'venue': "binance", 'account': "subaccount4"})
        )[0]['position_value']
        result2 = list(
            self.db['active_digital']['test_positions'].find({'client': "rundmc", 'venue': "binance", 'account': "subaccount6"})
        )[0]['position_value']

        # Iterate over the result and compare the values
        self.assertEqual(result1, self.seta_data['subaccount4'])
        self.assertEqual(result2, self.seta_data['subaccount6'])

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