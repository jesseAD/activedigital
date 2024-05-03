import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['bid_asks']
active_digita = mongo_client['active_digita']['bid_asks']

exchanges = ['binance', 'okx', 'bybit']

for exchange in exchanges:
  bid_asks1 = list(active_digital.aggregate([
    {'$match': {'venue': exchange}},
    {'$group': {
      '_id': "$symbol",
      'symbol': {'$last': "$symbol"},
      'bid_ask_value': {'$last': "$bid_ask_value"}
    }}
  ]))
  bid_asks2 = list(active_digita.aggregate([
    {'$match': {'venue': exchange}},
    {'$group': {
      '_id': "$symbol",
      'symbol': {'$last': "$symbol"},
      'bid_ask_value': {'$last': "$bid_ask_value"}
    }}
  ]))

  print(exchange)

  dict1 = {
    item['symbol']: item['bid_ask_value'] for item in bid_asks1
  }
  dict2 = {
    item['symbol']: item['bid_ask_value'] for item in bid_asks2
  }

  for item in dict2:
    val1 = abs(dict1[item]['spot']['bid'] - dict2[item]['spot']['bid']) / dict1[item]['spot']['bid']
    val2 = abs(dict1[item]['spot']['ask'] - dict2[item]['spot']['ask']) / dict1[item]['spot']['ask']
    val3 = abs(dict1[item]['perp']['bid'] - dict2[item]['perp']['bid']) / dict1[item]['perp']['bid']
    val4 = abs(dict1[item]['perp']['ask'] - dict2[item]['perp']['ask']) / dict1[item]['perp']['ask']

    if val1 > 0.01 or val2 > 0.01 or val3 > 0.01 or val4 > 0.01:
      print("Not same")
      print(item)
      print(val1)
      print(val2)
      print(val3)
      print(val4)