import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.positions import Positions

class TestPositions(unittest.TestCase):
    def setUp(self):
        mongo_client = MongoClient('localhost', 27017)
        db = mongo_client['production']
        self.test_collection = db['test']
        self.test_collection.delete_many({})

    def test_singleExchangeSingleSubAccountPositionsStoredToMongoDb(self):
        with mock.patch('src.handlers.positions'):             
            position = Positions.create_test(
                exchange='binance',
                positionType='long',
                sub_account='subaccount1'
            )
            
            stored_result = Positions.get_test(
                exchange='binance',
                position_type='long',
                account='subaccount1'
            )

            assert position == stored_result

    def test_singleExchangeTwoSubAccountsPositionsStoredToMongoDb(self):
        with mock.patch('src.lib.config.get_data_collectors'):            
            position1 = Positions.create_test(
                exchange='binance',
                positionType='long',
                sub_account='subaccount1'
            )
            position2 = Positions.create_test(
                exchange='binance',
                positionType='long',
                sub_account='subaccount2'
            )
            
            stored_result1 = Positions.get_test(
                exchange='binance',
                position_type='long',
                account='subaccount1'
            )

            stored_result2 = Positions.get_test(
                exchange='binance',
                position_type='long',
                account='subaccount2'
            )

            assert position1 == stored_result1 and position2 == stored_result2

    def test_twoExchangeSingleSubAccountsPositionsStoredToMongoDb(self):
        with mock.patch('src.lib.config.get_data_collectors'):            
            position1 = Positions.create_test(
                exchange='binance',
                positionType='long',
                sub_account='subaccount1'
            )
            position2 = Positions.create_test(
                exchange='okx',
                positionType='long',
                sub_account='subaccount1'
            )
            
            stored_result1 = Positions.get_test(
                exchange='binance',
                position_type='long',
                account='subaccount1'
            )
            stored_result2 = Positions.get_test(
                exchange='okx',
                position_type='long',
                account='subaccount1'
            )

            assert position1 == stored_result1 and position2 == stored_result2

    def test_twoExchangeaTwoSubAccountsPositionsStoredToMongoDb(self):
        position1 = Positions.create_test(
            exchange='binance',
            positionType='long',
            sub_account='subaccount1'
        )
        position2 = Positions.create_test(
            exchange='binance',
            positionType='long',
            sub_account='subaccount2'
        )
        position3 = Positions.create_test(
            exchange='okx',
            positionType='long',
            sub_account='subaccount1'
        )
        
        stored_result1 = Positions.get_test(
            exchange='binance',
            position_type='long',
            account='subaccount1'
        )
        stored_result2 = Positions.get_test(
            exchange='binance',
            position_type='long',
            account='subaccount2'
        )
        stored_result3 = Positions.get_test(
            exchange='okx',
            position_type='long',
            account='subaccount1'
        )

        assert position1 == stored_result1 and position2 == stored_result2 and position3 == stored_result3

if __name__ == '__main__':
    unittest.main()