import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['index_prices']
active_digita = mongo_client['active_digita']['index_prices']

exchanges = ['binance', 'okx', 'bybit']

for exchange in exchanges:
  index_prices1 = list(active_digital.aggregate([
    {'$match': {'venue': exchange}},
    {'$group': {
      '_id': "$symbol",
      'symbol': {'$last': "$symbol"},
      'index_price_value': {'$last': "$index_price_value"}
    }}
  ]))
  index_prices2 = list(active_digita.aggregate([
    {'$match': {'venue': exchange}},
    {'$group': {
      '_id': "$symbol",
      'symbol': {'$last': "$symbol"},
      'index_price_value': {'$last': "$index_price_value"}
    }}
  ]))

  print(exchange)

  dict1 = {
    item['symbol']: item['index_price_value'] for item in index_prices1
  }
  dict2 = {
    item['symbol']: item['index_price_value'] for item in index_prices2
  }

  for item in dict2:
    if abs(float(dict1[item]['indexPrice']) - float(dict2[item]['indexPrice'])) / float(dict1[item]['indexPrice']) > 0.01:
      print("Not same")
      print(item)