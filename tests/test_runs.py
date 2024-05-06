import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

import os
import sys

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))

sys.path.append(target_dir)

from src.handlers.balances import Balances
from src.handlers.positions import Positions
from src.handlers.leverages import Leverages
from src.handlers.runs import Runs
from src.config import read_config_file

class TestRuns(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')

        mongo_uri = None

        if os.getenv("mode") == "prod":
            mongo_uri = 'mongodb+srv://activedigital:'+os.getenv("CLOUD_MONGO_PASSWORD")+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
        self.db = MongoClient(mongo_uri)

        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)
        with open('tests/tdin.json') as tdin:
           self.tdin_data = json.load(tdin)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_PositionsAndBalancedStoredToMongoDbWithSameRunID(self, mock_runs):
        self.db['active_digital']['test_positions'].delete_many({})
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        # Call mock create function using existing json data
        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )

        dbPositions =  self.db['active_digital']['test_positions'].find()
        positionRunId= None
        for dbPosition in dbPositions:
            if positionRunId is None:
                positionRunId=dbPosition['runid']
            else:
                self.assertEqual(positionRunId, dbPosition['runid'])
                positionRunId = dbPosition['runid']


        mock_create.return_value = {'balance_value': self.sesa_data['balances']}

        # Call mock create function using existing json data
        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )

        dbBalances = self.db['active_digital']['test_balances'].find()
        balanceRunId = None
        for dbBalance in dbBalances:
            if balanceRunId is None:
                balanceRunId = dbBalance['runid']
            else:
                self.assertEqual(balanceRunId, dbBalance['runid'])
                balanceRunId = dbBalance['runid']


        self.assertEqual(positionRunId, balanceRunId)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_ConsecutivePositionsAndBalancesStoredWithIncrementedRunID(self, mock_runs):
        self.db['active_digital']['test_positions'].delete_many({})
        self.db['active_digital']['test_balances'].delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        # Call mock create function using existing json data
        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )

        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )

        prevPositionsRunId = self.db['active_digital']['test_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        prevBalancesRunId = self.db['active_digital']['test_balances'].find().sort('runid', -1).limit(1)[0]['runid']

        Runs(self.db['active_digital'], "runs").start()

        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='binance',
            sub_account='subaccount4',
            position_value=self.seta_data['positions']['subaccount4']
        )

        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='binance',
            sub_account='subaccount4',
            balanceValue=self.seta_data['balances']['subaccount4']
        )

        nextPositionsRunId = self.db['active_digital']['test_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        nextBalancesRunId = self.db['active_digital']['test_balances'].find().sort('runid', -1).limit(1)[0]['runid']
        
        self.assertEqual(prevPositionsRunId + 1, nextPositionsRunId)
        self.assertEqual(prevBalancesRunId + 1, nextBalancesRunId)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_LeverageStoredWithSameRunIDAsPositionsAndBalances(self, mock_runs):
        self.db['active_digital']['test_positions'].delete_many({})
        self.db['active_digital']['test_balances'].delete_many({})
        self.db['active_digital']['test_leverages'].delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        # Call mock create function using existing json data
        Positions(self.db, "test_positions").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )

        Balances(self.db, "test_balances").create(
            client='rundmc',
            exchange='okx',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )

        Leverages(self.db['active_digital'], "test_leverages").get(
            client='rundmc',
            exchange='okx',
            account='subaccount1',
            leverage=1.0
        )

        positionsRunId = self.db['active_digital']['test_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        balancesRunId = self.db['active_digital']['test_balances'].find().sort('runid', -1).limit(1)[0]['runid']
        leveragesRunId = self.db['active_digital']['test_leverages'].find().sort('runid', -1).limit(1)[0]['runid']

        self.assertEqual(leveragesRunId, positionsRunId)
        self.assertEqual(leveragesRunId, balancesRunId)


    # @mock.patch('src.handlers.positions.Positions', autospec=True)
    # def test_NewPositionsBalancesAndLeverageRunIgnoresPreviousParitalData(self, mock_runs):
    #     self.db['active_digital']['test_positions'].delete_many({})
    #     self.db['active_digital']['test_balances'].delete_many({})
    #     self.db['active_digital']['test_leverages'].delete_many({})
    #     mock_create = mock_runs.return_value.create
    #     mock_create.return_value = {'position_value': self.tdin_data['positions']}

    #     # Call mock create function using existing json data
    #     Positions("test_positions").create(
    #         client='deepspace',
    #         exchange='binance',
    #         sub_account='subaccount1',
    #         position_value=self.tdin_data['positions']
    #     )

    #     Balances("test_balances").create(
    #         client='deepspace',
    #         exchange='binance',
    #         sub_account='subaccount1',
    #         balanceValue=self.tdin_data['balances']
    #     )

    #     leverage_value = {
    #         "client": 'deepspace',
    #         "venue": 'binance',
    #         "account": 'subaccount1',
    #         "timestamp": datetime.now(timezone.utc),
    #     }
    #     leverage_value['runid'] = self.db['runs'].find().sort('runid', -1).limit(1)[0]['runid']

    #     try:
    #         max_notional = abs(float(max(self.tdin_data['positions'], key=lambda x: abs(float(x['notional'])))['notional']))
    #     except:
    #         return False
            
    #     base_currency = self.config['balances']['base_ccy']

    #     balance_in_base_currency = 0
    #     for currency, balance in self.tdin_data['balances'][0].items():
    #         if currency == base_currency:
    #             balance_in_base_currency += balance
    #         else:
    #             balance_in_base_currency += self.tdin_data['tickers'] * balance
    #     try:
    #         leverage_value['leverage'] = max_notional / balance_in_base_currency
    #     except:
    #         return False

    #     self.db[self.config['mongo_db_collection'] + '_leverages'].insert_one(leverage_value)

    #     positionsRunId = self.db[self.config['mongo_db_collection'] + '_positions'].find().sort('runid', -1).limit(1)[0]['runid']
    #     balancesRunId = self.db[self.config['mongo_db_collection'] + '_balances'].find().sort('runid', -1).limit(1)[0]['runid']
    #     leveragesRunId = self.db[self.config['mongo_db_collection'] + '_leverages'].find().sort('runid', -1).limit(1)[0]['runid']

    #     self.assertNotEqual(leveragesRunId, positionsRunId)
    #     self.assertNotEqual(leveragesRunId, balancesRunId)



if __name__ == '__main__':
    unittest.main()