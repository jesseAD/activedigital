import json
import unittest
from unittest import mock
from pymongo import MongoClient

from src.handlers.balances import Balances
from src.handlers.positions import Positions
from src.config import read_config_file

class TestRuns(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')
        self.mongo_client = MongoClient(self.config['mongo_host'], self.config['mongo_port'])
        self.db = self.mongo_client['active_digital']
        self.test_collection = self.db[self.config['mongo_db_collection']+'_positions']
        self.test_collection.delete_many({})

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)['positions']
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)['positions']

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_PositionsAndBalancedStoredToMongoDbWithSameRunID(self, mock_positions):
        self.test_collection.delete_many({})
        mock_create = mock_positions.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data}

        # Call mock create function using existing json data
        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data
        )['position_value']

        positions = Positions("test_positions").get(client='deepspace', exchange='binance', account='subaccount1')[0]['position_value']
        dbPositions =   self.db[self.config['mongo_db_collection']+'_positions'].find()
        positionRunId= None
        for dbPosition in dbPositions:
            if positionRunId is None:
                positionRunId=dbPosition['runid']
            else:
                self.assertEqual(positionRunId, dbPosition['runid'])
                positionRunId = dbPosition['runid']


        mock_create.return_value = {'balance_value': self.sesa_data}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data
        )['balance_value']

        balances = Balances("test_balances").get(exchange='binance', account='subaccount1')[0]['balance_value']
        dbBalances = self.db[self.config['mongo_db_collection'] + '_balances'].find()
        balanceRunId = None
        for dbBalance in dbBalances:
            if balanceRunId is None:
                balanceRunId = dbBalance['runid']
            else:
                self.assertEqual(balanceRunId, dbBalance['runid'])
                balanceRunId = dbBalance['runid']


        self.assertEqual(positionRunId, balanceRunId)



if __name__ == '__main__':
    unittest.main()