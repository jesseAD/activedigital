import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['roll_costs']
active_digita = mongo_client['active_digita']['roll_costs']

exchanges = ['binance', 'okx', 'bybit']

for exchange in exchanges:
  roll_costs1 = list(active_digital.aggregate([
    {'$match': {'venue': exchange}},
    {'$group': {
      '_id': {'symbol': "$roll_cost_value.symbol", 'prompt': "$roll_cost_value.prompt", 'type': "$roll_cost_value.type"},
      'roll_cost_value': {'$last': "$roll_cost_value"}
    }},
    {'$sort': {
      "roll_cost_value.symbol": 1,
      "roll_cost_value.prompt": 1,
      "roll_cost_value.type": 1,
    }}
  ]))
  roll_costs2 = list(active_digita.aggregate([
    {'$match': {'venue': exchange}},
    {'$group': {
      '_id': {'symbol': "$roll_cost_value.symbol", 'prompt': "$roll_cost_value.prompt", 'type': "$roll_cost_value.type"},
      'roll_cost_value': {'$last': "$roll_cost_value"}
    }},
    {'$sort': {
      "roll_cost_value.symbol": 1,
      "roll_cost_value.prompt": 1,
      "roll_cost_value.type": 1,
    }}
  ]))
  roll_costs1 = [item['roll_cost_value'] for item in roll_costs1]
  roll_costs2 = [item['roll_cost_value'] for item in roll_costs2]

  print(exchange)

  for i in range(len(roll_costs2)):
    if (
      roll_costs1[i]['contract'] != roll_costs2[i]['contract'] or
      abs(roll_costs1[i]['carry_cost'] - roll_costs2[i]['carry_cost']) / roll_costs1[i]['carry_cost'] > 0.01 or
      abs(roll_costs1[i]['term_structure'] - roll_costs2[i]['term_structure']) / roll_costs1[i]['term_structure'] > 0.01
    ):
      print(roll_costs1[i]['symbol'] + " " + roll_costs1[i]['prompt'] + " " + roll_costs1[i]['type'])
      print(roll_costs1[i])
      print(roll_costs2[i])