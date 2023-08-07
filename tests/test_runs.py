import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.balances import Balances
from src.handlers.positions import Positions
from src.handlers.tickers import Tickers
from src.config import read_config_file

class TestRuns(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')
        self.mongo_client = MongoClient(self.config['mongo_host'], self.config['mongo_port'])
        self.db = self.mongo_client['active_digital']
        self.test_collection = self.db[self.config['mongo_db_collection']+'_positions']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_balances']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_instruments']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_leverages']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_tickers']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_runs']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_test']
        self.test_collection.delete_many({})
        # read hard-coded values
        with open('tests/sesa.json') as sesa:
           self.sesa_data = json.load(sesa)
        with open('tests/seta.json') as seta:
           self.seta_data = json.load(seta)
        with open('tests/tdin.json') as tdin:
           self.tdin_data = json.load(tdin)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_PositionsAndBalancedStoredToMongoDbWithSameRunID(self, mock_runs):
        self.test_collection.delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        # Call mock create function using existing json data
        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
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


        mock_create.return_value = {'balance_value': self.sesa_data['balances']}

        # Call mock create function using existing json data
        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
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

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_ConsecutivePositionsAndBalancesStoredWithIncrementedRunID(self, mock_runs):
        self.test_collection.delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        # Call mock create function using existing json data
        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )['position_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )['balance_value']

        prevPositionsRunId = self.db[self.config['mongo_db_collection'] + '_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        prevBalancesRunId = self.db[self.config['mongo_db_collection'] + '_balances'].find().sort('runid', -1).limit(1)[0]['runid']

        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.seta_data['positions']['subaccount1']
        )['position_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.seta_data['balances']['subaccount1']
        )['balance_value']

        nextPositionsRunId = self.db[self.config['mongo_db_collection'] + '_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        nextBalancesRunId = self.db[self.config['mongo_db_collection'] + '_balances'].find().sort('runid', -1).limit(1)[0]['runid']
        
        self.assertEqual(prevPositionsRunId + 1, nextPositionsRunId)
        self.assertEqual(prevBalancesRunId + 1, nextBalancesRunId)

    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_LeverageStoredWithSameRunIDAsPositionsAndBalances(self, mock_runs):
        self.test_collection.delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        # Call mock create function using existing json data
        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )['position_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.sesa_data['balances']
        )['balance_value']

        Tickers("test_tickers").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            symbol='BTC/USDT',
            tickerValue=self.sesa_data['tickers'][0]
        )['ticker_value']

        leverage_value = {
            "client": 'deepspace',
            "venue": 'binance',
            "account": 'subaccount1',
            "timestamp": datetime.now(timezone.utc),
        }
        leverage_value['runid'] = self.db['runs'].find().sort('runid', -1).limit(1)[0]['runid']

        max_notional = abs(float(max(self.sesa_data['positions'], key=lambda x: abs(float(x['notional'])))['notional']))
            
        base_currency = self.config['balances']['base_ccy']

        balance_in_base_currency = 0
        for currency, balance in self.sesa_data['balances'][0].items():
            if currency == base_currency:
                balance_in_base_currency += balance
            else:
                balance_in_base_currency += self.sesa_data['tickers'] * balance
        leverage_value['leverage'] = max_notional / balance_in_base_currency

        self.db[self.config['mongo_db_collection'] + '_leverages'].insert_one(leverage_value)

        positionsRunId = self.db[self.config['mongo_db_collection'] + '_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        balancesRunId = self.db[self.config['mongo_db_collection'] + '_balances'].find().sort('runid', -1).limit(1)[0]['runid']
        leveragesRunId = self.db[self.config['mongo_db_collection'] + '_leverages'].find().sort('runid', -1).limit(1)[0]['runid']

        self.assertEqual(leveragesRunId, positionsRunId)
        self.assertEqual(leveragesRunId, balancesRunId)


    @mock.patch('src.handlers.positions.Positions', autospec=True)
    def test_NewPositionsBalancesAndLeverageRunIgnoresPreviousParitalData(self, mock_runs):
        self.test_collection.delete_many({})
        mock_create = mock_runs.return_value.create
        mock_create.return_value = {'position_value': self.tdin_data['positions']}

        # Call mock create function using existing json data
        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.tdin_data['positions']
        )['position_value']

        Balances("test_balances").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            balanceValue=self.tdin_data['balances']
        )['balance_value']

        Tickers("test_tickers").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            symbol='BTC/USDT',
            tickerValue=self.tdin_data['tickers'][0]
        )['ticker_value']

        leverage_value = {
            "client": 'deepspace',
            "venue": 'binance',
            "account": 'subaccount1',
            "timestamp": datetime.now(timezone.utc),
        }
        leverage_value['runid'] = self.db['runs'].find().sort('runid', -1).limit(1)[0]['runid']

        try:
            max_notional = abs(float(max(self.tdin_data['positions'], key=lambda x: abs(float(x['notional'])))['notional']))
        except:
            return False
            
        base_currency = self.config['balances']['base_ccy']

        balance_in_base_currency = 0
        for currency, balance in self.tdin_data['balances'][0].items():
            if currency == base_currency:
                balance_in_base_currency += balance
            else:
                balance_in_base_currency += self.tdin_data['tickers'] * balance
        try:
            leverage_value['leverage'] = max_notional / balance_in_base_currency
        except:
            return False

        self.db[self.config['mongo_db_collection'] + '_leverages'].insert_one(leverage_value)

        positionsRunId = self.db[self.config['mongo_db_collection'] + '_positions'].find().sort('runid', -1).limit(1)[0]['runid']
        balancesRunId = self.db[self.config['mongo_db_collection'] + '_balances'].find().sort('runid', -1).limit(1)[0]['runid']
        leveragesRunId = self.db[self.config['mongo_db_collection'] + '_leverages'].find().sort('runid', -1).limit(1)[0]['runid']

        self.assertNotEqual(leveragesRunId, positionsRunId)
        self.assertNotEqual(leveragesRunId, balancesRunId)



if __name__ == '__main__':
    unittest.main()