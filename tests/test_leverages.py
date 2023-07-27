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

from src.handlers.positions import Positions
from src.handlers.balances import Balances
from src.handlers.leverages import Leverages
from src.config import read_config_file

class TestLeverages(unittest.TestCase):
    def setUp(self):
        config = read_config_file('tests/config.yaml')
        mongo_client = MongoClient(config['mongo_host'], config['mongo_port'])
        db = mongo_client['active_digital']
        self.test_collection = db[config['mongo_db_collection']+'_leverages']
        self.test_collection.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
            self.sesa_data = json.load(sesa)
            
        # with open('tests/sesa.json') as sesa:
        #     self.sesa_data_balances = json.load(sesa)['balances']

    @mock.patch('src.handlers.leverages.Leverages', autospec=True)
    def test_PositionsAndBalancesStoredWithSameRunID(self, mock_leverages):
        self.test_collection.delete_many({})
        mock_create = mock_leverages.return_value.get
        mock_create.return_value = {'leverage_value': self.sesa_data['leverages']}
        # Call mock create function using existing json data
        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            positionType='long',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )['position_value']
        position_runid = Positions("test_positions").get(client='deepspace', exchange='binance', account='subaccount1')[0]['runid']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )['balance_value']
        balance_runid = Balances("test_balances").get(exchange='binance', account='subaccount1')[0]['runid']
        self.assertEqual(position_runid, balance_runid)

if __name__ == '__main__':
    unittest.main()