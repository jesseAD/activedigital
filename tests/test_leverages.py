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
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
            self.sesa_data = json.load(sesa)
            
        # with open('tests/sesa.json') as sesa:
        #     self.sesa_data_balances = json.load(sesa)['balances']

    @mock.patch('src.handlers.leverages.Leverages', autospec=True)
    def test_PositionsAndBalancesStoredWithSameRunID(self, mock_leverages):
        self.db['active_digital']['test_positions'].delete_many({})
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_leverages.return_value.get
        mock_create.return_value = {'leverage_value': self.sesa_data['leverages']}
        # Call mock create function using existing json data
        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )
        position_runid = list(
            self.db['active_digital']['test_positions'].find({'client': 'rundmc', 'venue': 'okx', 'account': 'subaccount1'})
        )[0]['runid']

        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )
        balance_runid = list(
            self.db['active_digital']['test_balances'].find({'client': 'rundmc', 'venue': 'okx', 'account': 'subaccount1'})
        )[0]['runid']
        self.assertEqual(position_runid, balance_runid)

if __name__ == '__main__':
    unittest.main()