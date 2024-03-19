import json
import os, sys
import unittest
from unittest import mock
from datetime import datetime, timezone

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.lib.expiry_date import get_expiry_date


class TestExpiryDate(unittest.TestCase):
  def setUp(self) -> None:
    return super().setUp()
  
  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_1st_of_month(self, mock_expiry):

    date = datetime.strptime("2024-03-01T07:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-03-01T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-03-08T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-03-29T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)

  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_before_2_weeks_out(self, mock_expiry):

    date = datetime.strptime("2024-03-15T07:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-03-15T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-03-22T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-03-29T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)

  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_within_2_weeks(self, mock_expiry):

    date = datetime.strptime("2024-03-15T08:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-03-22T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-03-29T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-05-31T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)

  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_before_1_week_out(self, mock_expiry):

    date = datetime.strptime("2024-03-22T07:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-03-22T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-03-29T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-05-31T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)

  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_within_1_week(self, mock_expiry):

    date = datetime.strptime("2024-03-22T08:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-03-29T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-04-05T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-05-31T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)

  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_before_1_day_out(self, mock_expiry):

    date = datetime.strptime("2024-03-29T07:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-03-29T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-04-05T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-05-31T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)

  @mock.patch('src.lib.expiry_date.get_expiry_date', autospec=True)
  def test_within_1_day(self, mock_expiry):

    date = datetime.strptime("2024-03-29T08:28:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    weeklyExpiry = datetime.strptime("2024-04-05T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextWeekExpiry = datetime.strptime("2024-04-12T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    monthlyExpiry = datetime.strptime("2024-04-26T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextMonthExpiry = datetime.strptime("2024-05-31T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    quaterExpiry = datetime.strptime("2024-06-28T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")
    nextQuarterExpiry = datetime.strptime("2024-09-27T08:00:30+00:00", "%Y-%m-%dT%H:%M:%S%z")

    weeklyResult = get_expiry_date("THISWEEK", date)
    nextWeekResult = get_expiry_date("NEXTWEEK", date)
    monthlyResult = get_expiry_date("THISMONTH", date)
    nextMonthResult = get_expiry_date("NEXTMONTH", date)
    quaterResult = get_expiry_date("QUARTER", date)
    nextQuarterResult = get_expiry_date("NEXTQUARTER", date)

    assert(weeklyExpiry == weeklyResult)
    assert(nextWeekExpiry == nextWeekResult)
    assert(monthlyExpiry == monthlyResult)
    assert(nextMonthExpiry == nextMonthResult)
    assert(quaterExpiry == quaterResult)
    assert(nextQuarterExpiry == nextQuarterResult)


if __name__ == '__main__':
    unittest.main()