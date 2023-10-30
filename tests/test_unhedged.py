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
        spot = self.data['testSingleSidedSpotPositionAggregatesToOnePosition']['spot']
        # perp = self.data['testSingleSidedSpotPositionAggregatesToOnePosition']['perp']

        res = get_unhedged(spot=spot)

        self.assertEqual(1, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotPositionAggregatesToTwoPositions(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        positions = self.data['testMultipleSidedSpotPositionAggregatesToTwoPositions']['spot']

        res = get_unhedged(spot=positions)

        self.assertEqual(2, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotAndPerpPositionAggregatesToTwoPositions(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        spot = self.data['testMultipleSidedSpotAndPerpPositionAggregatesToTwoPositions']['spot']
        perp = self.data['testMultipleSidedSpotAndPerpPositionAggregatesToTwoPositions']['perp']

        res = get_unhedged(perp, spot)

        self.assertEqual(2, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotAndPerpPositionAggregatesToTwoPositions1(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        perp = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo16Positions1']['perp']
        spot = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo16Positions1']['spot']

        res = get_unhedged(perp, spot)

        self.assertEqual(10, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleSidedSpotAndPerpPositionAggregatesToTwoPositions2(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        spot = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo16Positions2']['spot']
        perp = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo16Positions2']['perp']

        res = get_unhedged(perp, spot)

        self.assertEqual(10, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding1(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        perp = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding1']['perp']
        spot = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding1']['spot']

        res = get_unhedged(perp, spot)

        self.assertEqual(2, len(res))

    @mock.patch('src.lib.unhedged.get_unhedged', autospec=True)
    def test_MultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding2(self, mock_unhedged):
        mock_create = mock_unhedged.return_value.create
        mock_create.return_value = {'data': self.data}

        # Call mock create function using existing json data
        perp = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding2']['perp']
        spot = self.data['testMultipleDoubleSidedSpotAndPerpPositionAggregatesTo3UnbalancedPositionsDueToRounding2']['spot']

        res = get_unhedged(perp, spot)

        self.assertEqual(2, len(res))


if __name__ == '__main__':
    unittest.main()