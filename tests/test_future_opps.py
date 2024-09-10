import os, sys
import json
import pytest
import unittest
from unittest import mock
from pymongo import MongoClient
from datetime import datetime, timezone

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.lib.apr_pairs import make_pairs, filter_insts
from src.config import read_config_file

with open('tests/future_opps.json') as data:
  insts = json.load(data)

def test_FutureOpps():
  res = make_pairs(insts=insts['total'])

  assert(38 == len(res))

def test_NoPairForLinearAndInverseWithSameDate():
  res = make_pairs(insts=insts['same_date'])

  assert(len(res) == 0)