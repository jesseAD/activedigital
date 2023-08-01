import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.handlers.instruments import Instruments
from src.handlers.positions import Positions
from src.config import read_config_file

class TestRuns(unittest.TestCase):
    def setUp(self):
        self.config = read_config_file('tests/config.yaml')
        self.mongo_client = MongoClient(self.config['mongo_host'], self.config['mongo_port'])        
        self.db = self.mongo_client['active_digital']
        self.runs_db = self.db['runs']
        self.test_collection = self.db[self.config['mongo_db_collection']+'_positions']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_balances']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_instruments']
        self.test_collection.delete_many({})
        self.test_collection = self.db[self.config['mongo_db_collection']+'_leverages']
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

    @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    def test_SnapshotDataNotStoredWhenNoChange(self, mock_storetypes):
        mock_create = mock_storetypes.return_value.create
        mock_create.return_value = {'instrument_value': self.sesa_data['instruments']}

        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            instrumentValue=self.sesa_data['instruments']
        )

        dbInstruments = self.db[self.config['mongo_db_collection'] + '_instruments'].find()
        prevRunId = None
        for dbInstrument in dbInstruments:
            if prevRunId is None:
                prevRunId = dbInstrument['runid']
            else:
                self.assertEqual(prevRunId, dbInstrument['runid'])
                prevRunId = dbInstrument['runid']


        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid'] + 1
            except:
                pass
        self.runs_db.insert_one({"start_time": datetime.now(timezone.utc), "runid": latest_run_id})

        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            instrumentValue=self.sesa_data['instruments']
        )

        dbInstruments = self.db[self.config['mongo_db_collection'] + '_instruments'].find()
        currentRunId = None
        for dbInstrument in dbInstruments:
            if currentRunId is None:
                currentRunId = dbInstrument['runid']
            else:
                self.assertEqual(currentRunId, dbInstrument['runid'])
                currentRunId = dbInstrument['runid']
        
        self.assertEqual(prevRunId, currentRunId)

    @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    def test_SnapshotdataOverwittenWhenChanges(self, mock_storetypes):
        mock_create = mock_storetypes.return_value.create
        mock_create.return_value = {'instrument_value': self.seta_data['instruments']['subaccount1']}

        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            instrumentValue=self.sesa_data['instruments']
        )

        Instruments("test_instruments").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            instrumentValue=self.seta_data['instruments']['subaccount1']
        )

        latestInstrument = self.db[self.config['mongo_db_collection'] + '_instruments'].find_one(
            {
                'client': 'deepspace',
                'venue': 'binance',
                'account': 'subaccount1'
            }
        )
        
        self.assertEqual(self.seta_data['instruments']['subaccount1'][0]["info"], latestInstrument['instrument_value'])

    @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    def test_TimeseriesdataAppendedWhenChanges(self, mock_storetypes):
        mock_create = mock_storetypes.return_value.create
        mock_create.return_value = {'position_value': self.seta_data['positions']['subaccount1']}

        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = -1
        for item in run_ids:
            try:
                latest_run_id = item['runid'] + 1
            except:
                pass
        self.runs_db.insert_one({"start_time": datetime.now(timezone.utc), "runid": latest_run_id})

        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.seta_data['positions']['subaccount1']
        )

        latest_value = None
        position_values = self.db[self.config['mongo_db_collection'] + '_positions'].find().sort('runid', -1).limit(1)
        latest_run_id = -1
        for item in position_values:
            if latest_run_id < item['runid']:
                latest_run_id = item['runid']
                latest_value = item['position_value'] 
        
        self.assertEqual(self.seta_data['positions']['subaccount1'], latest_value)

    @mock.patch('src.handlers.instruments.Instruments', autospec=True)
    def test_TimeseriesdataNotAppendedWhenNoChange(self, mock_storetypes):
        mock_create = mock_storetypes.return_value.create
        mock_create.return_value = {'position_value': self.sesa_data['positions']}

        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )

        dbPositions = self.db[self.config['mongo_db_collection'] + '_positions'].find()
        prevRunId = None
        for dbPosition in dbPositions:
            if prevRunId is None:
                prevRunId = dbPosition['runid']
            else:
                self.assertEqual(prevRunId, dbPosition['runid'])
                prevRunId = dbPosition['runid']

        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = -1
        for item in run_ids:
            try:
                latest_run_id = item['runid'] + 1
            except:
                pass
        self.runs_db.insert_one({"start_time": datetime.now(timezone.utc), "runid": latest_run_id})

        Positions("test_positions").create(
            client='deepspace',
            exchange='binance',
            sub_account='subaccount1',
            position_value=self.sesa_data['positions']
        )    

        dbPositions = self.db[self.config['mongo_db_collection'] + '_positions'].find()
        currentRunId = None
        for dbPosition in dbPositions:
            if currentRunId is None:
                currentRunId = dbPosition['runid']
            else:
                self.assertEqual(currentRunId, dbPosition['runid'])
                currentRunId = dbPosition['runid']
        
        self.assertEqual(prevRunId, currentRunId)

if __name__ == '__main__':
    unittest.main()