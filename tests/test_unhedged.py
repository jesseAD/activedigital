import json
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

from src.lib.unhedged import get_unhedged
from src.config import read_config_file

class TestUnhedged(unittest.TestCase):
    def setUp(self):
        # read hard-coded values
        with open('tests/unhedged.json') as data:
           self.data = json.load(data)

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_SingleSidedSpotPositionAggregatesToOnePosition(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testSingleSidedSpotPositionAggregatesToOnePosition']

        res = get_unhedged(positions)

        self.assertEqual(1, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotPositionAggregatesToTwoPositions(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleSidedSpotPositionAggregatesToTwoPositions']

        res = get_unhedged(positions)

        self.assertEqual(4, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotAndPerpPositionAggregatesToTwoPositions(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleSidedSpotAndPerpPositionAggregatesToTwoPositions']

        res = get_unhedged(positions)

        self.assertEqual(4, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotAndPerpPositionAggregatesToTwoPositions1(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo16Positions1']

        res = get_unhedged(positions)

        self.assertEqual(10, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotAndPerpPositionAggregatesToTwoPositions2(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo16Positions2']

        res = get_unhedged(positions)

        self.assertEqual(10, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding1(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding1']

        res = get_unhedged(positions)

        self.assertEqual(2, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding2(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding2']

        res = get_unhedged(positions)

        self.assertEqual(2, len(res))


if __name__ == '__main__':
    unittest.main()